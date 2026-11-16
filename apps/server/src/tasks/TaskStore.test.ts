import { assert, it } from "@effect/vitest";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { WorkTaskPersistedEvent } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as HashMap from "effect/HashMap";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { runMigrations } from "../persistence/Migrations.ts";
import * as TaskStore from "./TaskStore.ts";
import { reduceTaskCustody, reduceTaskFoundation } from "./TaskLifecycle.ts";

const decode = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);
const timestamp = "2026-11-16T00:00:00.000Z";
const definition = {
  revision: 1,
  title: "Outcome",
  objective: "Deliver",
  criteria: [{ id: "criterion", condition: "Done", verification: "Inspect" }],
  actionPolicy: { version: 1, rules: [] },
};
const record = (
  sequence: number,
  event: Record<string, unknown> = {},
  commandId: string | null = `command-${sequence}`,
) =>
  decode({
    event: {
      id: `event-${sequence}`,
      taskId: "task",
      occurredAt: timestamp,
      attribution: { type: "server" },
      definitionRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
      type: "task.created",
      projectId: "project",
      definition,
      ...event,
    },
    context: {
      sequence,
      eventId: typeof event.id === "string" ? event.id : `event-${sequence}`,
      occurredAt: typeof event.occurredAt === "string" ? event.occurredAt : timestamp,
      commandId,
    },
  });
const insert = Effect.fn("TaskStoreTest.insertNative")(function* (
  value: WorkTaskPersistedEvent,
  version: number,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
    VALUES(${value.context.sequence},json_extract(${JSON.stringify(value.event.id)},'$'),'task',json_extract(${JSON.stringify(value.event.taskId)},'$'),${version},${value.event.type},${value.context.occurredAt},json_extract(${JSON.stringify(value.context.commandId)},'$'),'system',${JSON.stringify(value.event)},'{}',2)`;
});
const db = NodeSqliteClient.layer({ filename: ":memory:" });
const testLayer = TaskStore.layer.pipe(Layer.provideMerge(db));
const configuration = {
  type: "task.configuration-updated",
  allowanceOverride: null,
  completionPolicy: { type: "inherit" },
};
const execution = (generation: number | null) => ({
  type: "task.execution-updated",
  desiredMode: "active",
  previousManagerGeneration: null,
  manager:
    generation === null
      ? null
      : { managerThreadId: `manager-${generation}`, managerGeneration: generation },
  reason: "recorded",
});

const seedWakeCustody = Effect.fn("TaskStoreTest.seedWakeCustody")(function* () {
  const store = yield* TaskStore.TaskStore;
  const created = yield* record(1);
  yield* insert(created, 0);
  yield* store.applyFoundation(created);
  const events = [
    { type: "task.manager-bound", managerThreadId: "manager", managerGeneration: 1 },
    {
      type: "task.allowance-configured",
      commandId: "command-3",
      previous: { type: "unconfigured", consumed: 0 },
      resulting: { type: "configured", cap: 2, consumed: 0 },
    },
    {
      type: "task.allowance-debited",
      commandId: "command-4",
      previous: { type: "configured", cap: 2, consumed: 0 },
      resulting: { type: "configured", cap: 2, consumed: 1 },
      allowanceUnit: 1,
    },
  ];
  const recorded: WorkTaskPersistedEvent[] = [];
  for (const [index, event] of events.entries()) {
    const value = yield* record(index + 2, event);
    yield* insert(value, index + 1);
    yield* store.applyManagerAllowance(value);
    recorded.push(value);
  }
  return recorded;
});
const wakeFixture = Effect.fn("TaskStoreTest.wakeFixture")(function* () {
  const recorded = yield* seedWakeCustody();
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO work_task_wake_state VALUES('task',0,0,NULL)`;
  let sequence = 4;
  const append = Effect.fn("TaskStoreTest.fixtureWakeProjection")(function* (
    input: Record<string, unknown>,
  ) {
    sequence++;
    const value = yield* record(sequence, { commandId: `command-${sequence}`, ...input });
    const event = value.event;
    yield* insert(value, sequence - 1);
    if (event.type === "task.wake-source-recorded") {
      const kind = JSON.stringify(event.source.kind),
        id = JSON.stringify(event.source.sourceId);
      const existing =
        yield* sql`SELECT 1 FROM work_task_wake_sources WHERE task_id='task' AND source_kind=json_extract(${kind},'$') AND source_id=json_extract(${id},'$')`;
      if (!existing.length) {
        yield* sql`INSERT INTO work_task_wake_sources VALUES('task',json_extract(${kind},'$'),json_extract(${id},'$'),${event.reservationId},${event.source.resultRef})`;
        yield* sql`INSERT INTO work_task_wake_source_state VALUES('task',json_extract(${kind},'$'),json_extract(${id},'$'),${event.id},${sequence},${event.dirtyGeneration},
          (SELECT count(*)<32 FROM work_task_wake_source_state WHERE task_id='task' AND retained_pending=1),${event.reservationId},${event.source.resultRef})`;
      }
    } else if ("reservationCommandId" in event) {
      const kind =
        event.type === "task.wake-reserved"
          ? "reserved"
          : event.type === "task.wake-claimed"
            ? "claimed"
            : event.type === "task.wake-claim-uncertain"
              ? "uncertain"
              : "acknowledged";
      if (kind === "reserved")
        yield* sql`INSERT INTO work_task_wake_reservations VALUES(${event.reservationId},'task',1,${event.reservationCommandId},NULL,NULL,1,${event.dirtyGeneration},${event.acknowledgedGeneration},${event.occurredAt},NULL)`;
      const parent = yield* sql<{
        claimed_at: string | null;
      }>`SELECT claimed_at FROM work_task_wake_reservations WHERE reservation_id=${event.reservationId}`;
      const claimed = event.type === "task.wake-claimed";
      yield* sql`INSERT INTO work_task_wake_phase_state VALUES(${event.id},${sequence},'task',${event.reservationId},${kind},${event.dirtyGeneration},${event.acknowledgedGeneration},
        ${event.type === "task.wake-acknowledged" ? event.previousAcknowledgedGeneration : null},${event.type === "task.wake-acknowledged" ? event.observedDirtyGeneration : null},
        ${claimed ? event.messageId : null},${claimed ? event.runId : null},${claimed ? (parent[0]!.claimed_at ?? event.occurredAt) : null})`;
      if (claimed && parent[0]!.claimed_at === null)
        yield* sql`UPDATE work_task_wake_reservations SET message_id=${event.messageId},run_id=${event.runId},claimed_at=${event.occurredAt} WHERE reservation_id=${event.reservationId}`;
    }
    // This fixture materializes decoded facts directly; AY has no production wake writer.
    yield* sql`UPDATE work_task_current_state SET last_applied_sequence=${sequence} WHERE task_id='task'`;
    yield* sql`UPDATE work_task_projections SET updated_at=${event.occurredAt} WHERE task_id='task'`;
    recorded.push(value);
    return value;
  });
  return {
    append: (input: Record<string, unknown>) => append(input).pipe(sql.withTransaction),
    recorded,
  };
});

const appendTaskFact = Effect.fn("TaskStoreTest.appendTaskFact")(function* (
  input: Record<string, unknown>,
) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* TaskStore.TaskStore;
  const taskId = typeof input.taskId === "string" ? input.taskId : "task";
  const [head] = yield* sql<{ sequence: number; version: number }>`SELECT
    COALESCE((SELECT max(sequence) FROM orchestration_events),0)+1 AS sequence,
    COALESCE((SELECT max(stream_version) FROM orchestration_events WHERE aggregate_kind='task' AND stream_id=json_extract(${JSON.stringify(taskId)},'$')),-1)+1 AS version`;
  const value = yield* record(head!.sequence, { commandId: `command-${head!.sequence}`, ...input });
  yield* insert(value, head!.version);
  if (value.event.type === "task.result-recorded") yield* store.applyResult(value);
  else if (value.event.type === "task.wait-updated") yield* store.applyWait(value);
  else if (value.event.type.startsWith("task.wake-")) yield* store.applyWake(value);
  else if (
    value.event.type.startsWith("task.allowance-") ||
    value.event.type.startsWith("task.manager-")
  )
    yield* store.applyManagerAllowance(value);
  else yield* store.applyFoundation(value);
  return value;
});
const wakeTables = [
  "work_task_results",
  "work_task_waits",
  "orchestration_events",
  "work_task_projections",
  "work_task_current_state",
  "work_task_view_fence",
  "work_task_manager_bindings",
  "work_task_allowances",
  "work_task_manager_reference_checkpoints",
  "work_task_wake_reservations",
  "work_task_wake_sources",
  "work_task_wake_state",
  "work_task_wake_source_state",
  "work_task_wake_phase_state",
];
const wakeSnapshot = Effect.fn("TaskStoreTest.wakeSnapshot")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows: Record<string, unknown> = {};
  for (const table of wakeTables)
    rows[table] = yield* sql`SELECT * FROM ${sql(table)} ORDER BY 1,2`;
  return rows;
});

it.effect("persists native result provenance and traverses a fixed history after new appends", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    assert.isUndefined(yield* store.readResults({ taskId: "unused" }));
    const history = [yield* record(1), ...(yield* seedWakeCustody())];
    assert.deepStrictEqual(yield* store.readResults({ taskId: "task" }), {
      records: [],
      throughSequence: 4,
      next: undefined,
    });
    const append = Effect.fn("TaskStoreTest.appendResultHistory")(function* (
      input: Record<string, unknown>,
    ) {
      const value = yield* appendTaskFact(input).pipe(sql.withTransaction);
      history.push(value);
      return value;
    });
    yield* append({
      type: "task.wake-source-recorded",
      source: { kind: "result", sourceId: "source", resultRef: null },
      reservationId: null,
      dirtyGeneration: 1,
    });
    const wait = {
      type: "task.wait-updated",
      waitId: "wait",
      managerThreadId: "manager",
      managerGeneration: 1,
      target: { type: "source", sourceId: "source" },
      state: "registered",
    };
    yield* append(wait);
    const result = {
      type: "task.result-recorded",
      sourceId: "source\0\ud800",
      summary: "Evidence\ud800",
      results: [
        { type: "artifact", uri: "artifact\0\ud800", hash: "hash" },
        { type: "run", threadId: "thread\ud800", runId: "run\0" },
        { type: "source_revision", repository: "repo", revision: "revision" },
      ],
    };
    const first = yield* append({ ...result, id: "r".repeat(2001) + "\ud800" });
    const revisions = {
      definitionRevision: 2,
      lifecycleRevision: 2,
      completionPolicyRevision: 2,
      actionPolicyRevision: 2,
    };
    yield* append({
      type: "task.definition-updated",
      ...revisions,
      definition: { ...definition, revision: 2, title: "New draft" },
    });
    yield* append({ ...configuration, ...revisions });
    const current = yield* store.readFoundation("task");
    const second = yield* append({
      ...result,
      contributorThreadId: "contributor\0\ud800",
      attribution: {
        type: "agent",
        threadId: "contributor\0\ud800",
        providerSessionId: "provider",
        runId: "run",
      },
    });
    const third = yield* append({ ...result, id: "a-third" });
    yield* append({ ...wait, state: "cleared" });
    const page = (yield* store.readResults({ taskId: "task", limit: 1 }))!;
    assert.deepStrictEqual(
      page.records.map((v) => v.recorded),
      [third],
    );
    const fourth = yield* append({ ...result, id: "z-fourth" });
    const next = (yield* store.readResults({ taskId: "task", limit: 1, after: page.next! }))!;
    assert.deepStrictEqual(
      next.records.map((v) => v.recorded),
      [second],
    );
    assert.equal(next.throughSequence, page.throughSequence);
    const last = (yield* store.readResults({ taskId: "task", limit: 1, after: next.next! }))!;
    assert.deepStrictEqual(
      last.records.map((v) => v.recorded),
      [first],
    );
    assert.isUndefined(last.next);
    assert.deepStrictEqual(
      (yield* store.readResults({
        taskId: "task",
        after: {
          taskId: "task",
          throughSequence: last.throughSequence,
          sequence: first.context.sequence,
          eventId: first.event.id,
        },
      }))?.records,
      [],
    );
    const newest = (yield* store.readResults({ taskId: "task", limit: 100 }))!;
    assert.deepStrictEqual(
      newest.records.map((v) => v.recorded),
      [fourth, third, second, first],
    );
    for (const value of [first, second, third, fourth])
      assert.deepStrictEqual(
        (yield* store.readResult({ taskId: "task", eventId: value.event.id }))?.recorded,
        value,
      );
    const after = (yield* store.readFoundation("task"))!;
    assert.deepStrictEqual(after.draft, current!.draft);
    assert.deepStrictEqual(after.projection.definition, current!.projection.definition);
    for (const key of [
      "definitionRevision",
      "lifecycleRevision",
      "completionPolicyRevision",
      "actionPolicyRevision",
    ] as const)
      assert.equal(after.projection[key], 2);
    let pure: ReturnType<typeof reduceTaskFoundation> | undefined;
    for (const value of history) {
      const event = value.event;
      switch (event.type) {
        case "task.created":
        case "task.definition-updated":
        case "task.configuration-updated":
        case "task.wait-updated":
        case "task.result-recorded":
          pure = reduceTaskFoundation(pure, { event, context: value.context });
      }
    }
    assert.deepStrictEqual(Array.from(pure!.results), [first, second, third, fourth]);
    const snapshot = yield* wakeSnapshot();
    for (const value of [first, second, third, fourth])
      assert.isFalse((yield* store.applyResult(value)).applied);
    assert.deepStrictEqual(yield* wakeSnapshot(), snapshot);
    assert.deepStrictEqual(
      yield* store.readResults({ taskId: "task" }).pipe(Effect.provide(TaskStore.layer)),
      newest,
    );
    assert.isUndefined(yield* store.readResult({ taskId: "unused", eventId: first.event.id }));
    const live = {
      foundation: after,
      results: newest,
      wait: yield* store.readWait({ taskId: "task", waitId: "wait" }),
      wake: yield* store.readWake("task"),
    };
    const rebuilt = yield* Effect.gen(function* () {
      yield* runMigrations();
      const target = yield* TaskStore.TaskStore;
      const database = yield* SqlClient.SqlClient;
      for (const [version, value] of history.entries()) yield* insert(value, version);
      const native = yield* database`SELECT * FROM orchestration_events ORDER BY sequence`;
      yield* target.rebuildCustodyProjection({
        taskId: "task",
        throughSequence: history.at(-1)!.context.sequence,
      });
      assert.deepStrictEqual(
        yield* database`SELECT * FROM orchestration_events ORDER BY sequence`,
        native,
      );
      return {
        foundation: yield* target.readFoundation("task"),
        results: yield* target.readResults({ taskId: "task" }),
        wait: yield* target.readWait({ taskId: "task", waitId: "wait" }),
        wake: yield* target.readWake("task"),
      };
    }).pipe(Effect.provide(Layer.fresh(testLayer)));
    assert.deepStrictEqual(rebuilt, live);
    yield* Effect.gen(function* () {
      yield* runMigrations();
      const target = yield* TaskStore.TaskStore;
      for (const [version, value] of history.entries()) yield* insert(value, version);
      const unsupported = yield* record(history.at(-1)!.context.sequence + 1, {
        type: "task.archived",
        archived: true,
      });
      yield* insert(unsupported, history.length);
      const before = yield* wakeSnapshot();
      assert.equal(
        (yield* Effect.exit(
          target.rebuildCustodyProjection({
            taskId: "task",
            throughSequence: unsupported.context.sequence,
          }),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
    }).pipe(Effect.provide(Layer.fresh(testLayer)));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects unproven result pages and atomically publishes exact result history", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seedWakeCustody();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const result = {
      type: "task.result-recorded",
      sourceId: "same",
      summary: "Evidence",
      results: [{ type: "artifact", uri: "artifact", hash: "hash" }],
    };
    const append = (input: Record<string, unknown>) =>
      appendTaskFact(input).pipe(sql.withTransaction);
    const first = yield* append(result);
    const second = yield* append(result);
    const third = yield* append(result);
    const page = (yield* store.readResults({ taskId: "task", limit: 1 }))!;
    const cursor = page.next!;
    const reject = Effect.fn("TaskStoreTest.rejectResultRead")(function* (
      operation: Effect.Effect<unknown, TaskStore.TaskProjectionError | TaskStore.TaskSourceError>,
    ) {
      const before = yield* wakeSnapshot();
      assert.equal((yield* Effect.exit(operation))._tag, "Failure");
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
    });
    for (const limit of [0, 101, 1.5, NaN, Infinity])
      yield* reject(store.readResults({ taskId: "task", limit }));
    for (const throughSequence of [
      -1,
      NaN,
      Number.MAX_SAFE_INTEGER + 1,
      third.context.sequence + 1,
    ])
      yield* reject(store.readResults({ taskId: "task", throughSequence }));
    for (const after of [
      { ...cursor, taskId: "other" },
      { ...cursor, throughSequence: cursor.throughSequence - 1 },
      { ...cursor, sequence: cursor.throughSequence + 1 },
      { ...cursor, sequence: NaN },
      { ...cursor, sequence: Number.MAX_SAFE_INTEGER + 1 },
      { ...cursor, sequence: second.context.sequence },
      { ...cursor, eventId: "missing" },
      { ...cursor, eventId: "event-1", sequence: 1 },
    ])
      yield* reject(
        store.readResults({ taskId: "task", throughSequence: page.throughSequence, after }),
      );
    const other = yield* append({ type: "task.created", taskId: "other\0\ud800", definition });
    yield* reject(store.readResults({ taskId: "task", throughSequence: other.context.sequence }));
    yield* reject(store.readResults({ taskId: "other\0\ud800", after: cursor }));
    const otherResult = yield* append({ ...result, taskId: "other\0\ud800" });
    assert.deepStrictEqual(
      (yield* store.readResult({ taskId: "other\0\ud800", eventId: otherResult.event.id }))
        ?.recorded,
      otherResult,
    );
    assert.isUndefined(yield* store.readResult({ taskId: "task", eventId: otherResult.event.id }));
    const [head] = yield* sql<{
      sequence: number;
      version: number;
    }>`SELECT (SELECT max(sequence)+1 FROM orchestration_events) AS sequence,max(stream_version)+1 AS version FROM orchestration_events WHERE stream_id='task'`;
    const pending = yield* record(head!.sequence, result);
    yield* insert(pending, head!.version);
    yield* reject(store.readResults({ taskId: "task", throughSequence: pending.context.sequence }));
    yield* reject(
      store.readResults({
        taskId: "task",
        after: { ...cursor, eventId: pending.event.id, sequence: pending.context.sequence },
      }),
    );
    assert.deepStrictEqual(
      (yield* store.readResults({ taskId: "task" }))?.records.map((v) => v.recorded),
      [third, second, first],
    );
    const applied = yield* Effect.all([store.applyResult(pending), store.applyResult(pending)], {
      concurrency: 2,
    });
    assert.equal(applied.filter((v) => v.applied).length, 1);
    yield* reject(store.readResults({ taskId: "task", throughSequence: other.context.sequence }));
    assert.deepStrictEqual(
      (yield* store.readResults({
        taskId: "task",
        throughSequence: page.throughSequence,
      }))?.records.map((v) => v.recorded),
      [third, second, first],
    );
    const altered = yield* record(
      first.context.sequence,
      { ...first.event, occurredAt: "2026-11-17T00:00:00Z" },
      first.context.commandId,
    );
    yield* reject(store.applyResult(altered));
    const corrupt = Effect.fn("TaskStoreTest.corruptResult")(function* (
      mutation: Effect.Effect<unknown, SqlError>,
      operation: Effect.Effect<unknown, TaskStore.TaskProjectionError | TaskStore.TaskSourceError>,
    ) {
      const before = yield* wakeSnapshot();
      let observed: string | undefined;
      yield* Effect.gen(function* () {
        yield* mutation.pipe(Effect.orDie);
        observed = (yield* Effect.exit(operation))._tag;
        return yield* Effect.fail("restore fixture");
      }).pipe(sql.withTransaction, Effect.ignore);
      assert.equal(observed, "Failure");
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
    });
    yield* corrupt(
      sql`UPDATE work_task_results SET summary='changed' WHERE event_id=${third.event.id}`,
      store.readResults({ taskId: "task", limit: 1 }),
    );
    yield* corrupt(
      sql`DELETE FROM work_task_results WHERE event_id=${pending.event.id}`,
      store.readResults({ taskId: "task" }),
    );
    yield* corrupt(
      sql`UPDATE work_task_results SET results_json='[]' WHERE event_id=${first.event.id}`,
      store.readResult({ taskId: "task", eventId: first.event.id }),
    );
    yield* corrupt(
      sql`UPDATE work_task_results SET source_id='changed' WHERE event_id=${first.event.id}`,
      store.applyResult(first),
    );
    yield* corrupt(
      sql`DELETE FROM orchestration_events WHERE event_id=${first.event.id}`,
      store.readResult({ taskId: "task", eventId: first.event.id }),
    );
    for (const table of [
      "work_task_current_state",
      "work_task_projections",
      "work_task_manager_reference_checkpoints",
      "work_task_view_fence",
    ]) {
      yield* sql.unsafe(
        `CREATE TEMP TRIGGER fail_result_publication BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(IGNORE); END`,
      );
      const before = yield* wakeSnapshot();
      assert.equal((yield* Effect.exit(append(result)))._tag, "Failure");
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
      yield* sql`DROP TRIGGER fail_result_publication`;
    }
    const before = yield* wakeSnapshot();
    assert.equal(
      (yield* Effect.exit(
        Effect.gen(function* () {
          yield* appendTaskFact(result);
          yield* appendTaskFact({ ...result, results: [] });
        }).pipe(sql.withTransaction),
      ))._tag,
      "Failure",
    );
    assert.deepStrictEqual(yield* wakeSnapshot(), before);
    yield* append(result);
    for (const query of [
      "SELECT event_id,sequence FROM work_task_results WHERE task_id='task' AND sequence<=100 ORDER BY sequence DESC,event_id DESC LIMIT 2",
      "SELECT event_id,sequence FROM work_task_results WHERE task_id='task' AND sequence<=100 AND (sequence,event_id)<(50,'event') ORDER BY sequence DESC,event_id DESC LIMIT 2",
      "SELECT * FROM work_task_results WHERE task_id='task' AND event_id='event'",
    ]) {
      const plans = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(plans.some((row) => row.detail.includes("SEARCH work_task_results")));
      assert.isFalse(plans.some((row) => /SCAN work_task_results|TEMP B-TREE/.test(row.detail)));
    }
    const empty = yield* store.readResults({ taskId: "task", throughSequence: 1 });
    assert.deepStrictEqual(empty, { records: [], throughSequence: 1, next: undefined });
    yield* Effect.gen(function* () {
      yield* runMigrations();
      const target = yield* TaskStore.TaskStore;
      const database = yield* SqlClient.SqlClient;
      const created = yield* record(1);
      yield* insert(created, 0);
      yield* target.applyFoundation(created);
      yield* database`INSERT INTO work_task_results VALUES('legacy','task','source',NULL,'{"type":"server"}','legacy','[{"type":"artifact","uri":"a","hash":"h"}]',1,${timestamp})`;
      const beforeLegacy = yield* wakeSnapshot();
      assert.equal((yield* Effect.exit(target.readResults({ taskId: "task" })))._tag, "Failure");
      assert.equal(
        (yield* Effect.exit(target.readResult({ taskId: "task", eventId: "legacy" })))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.exit(
          target.rebuildCustodyProjection({ taskId: "task", throughSequence: 1 }),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* wakeSnapshot(), beforeLegacy);
    }).pipe(Effect.provide(Layer.fresh(testLayer)));
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "persists historical waits without replacing current task state and rebuilds their lifecycle",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      assert.isUndefined(yield* store.readWait({ taskId: "unused", waitId: "wait" }));
      const history = [yield* record(1), ...(yield* seedWakeCustody())];
      const append = Effect.fn("TaskStoreTest.appendWaitHistory")(function* (
        input: Record<string, unknown>,
      ) {
        const value = yield* appendTaskFact(input).pipe(sql.withTransaction);
        history.push(value);
        return value;
      });
      const wait = {
        type: "task.wait-updated",
        managerThreadId: "manager",
        managerGeneration: 1,
        waitId: "wait\0\ud800",
        target: { type: "source", sourceId: "source\0\ud800" },
        state: "registered",
      };
      assert.isUndefined(yield* store.readWait({ taskId: "task", waitId: wait.waitId }));
      const original = yield* append(wait);
      const source = { kind: "result", sourceId: "wake", resultRef: null };
      yield* append({
        type: "task.wake-source-recorded",
        source,
        reservationId: null,
        dirtyGeneration: 1,
      });
      const reservation = {
        reservationId: "reservation",
        reservationCommandId: `command-${history.at(-1)!.context.sequence + 1}`,
        managerGeneration: 1,
        allowanceUnit: 1,
        dirtyGeneration: 1,
        acknowledgedGeneration: 0,
      };
      yield* append({
        type: "task.wake-reserved",
        ...reservation,
        pending: { sources: [source], overflowThroughGeneration: null },
      });
      yield* append({ type: "task.wake-claim-uncertain", ...reservation, reason: "lost response" });
      yield* append({
        type: "task.wake-claimed",
        ...reservation,
        messageId: "message",
        runId: "run",
        pending: { sources: [source], overflowThroughGeneration: null },
      });
      yield* append({
        type: "task.wake-acknowledged",
        ...reservation,
        previousAcknowledgedGeneration: 0,
        observedDirtyGeneration: 1,
        acknowledgedGeneration: 1,
      });
      yield* append({
        type: "task.manager-retired",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      yield* append({
        type: "task.manager-bound",
        managerThreadId: "manager-2",
        managerGeneration: 2,
      });
      const revisions = {
        definitionRevision: 2,
        lifecycleRevision: 2,
        completionPolicyRevision: 2,
        actionPolicyRevision: 2,
      };
      const draft = { ...definition, revision: 2, title: "Current draft" };
      yield* append({ type: "task.definition-updated", ...revisions, definition: draft });
      yield* append({ ...configuration, ...revisions });
      yield* append({ ...execution(2), ...revisions, desiredMode: "paused" });
      const current = (yield* store.readFoundation("task"))!;
      yield* append({ ...wait, state: "cleared" });
      const latest = yield* append({ ...wait, managerThreadId: "manager-2", managerGeneration: 2 });
      for (const [index, target] of [
        { type: "run", threadId: "thread\ud800", runId: "run\0" },
        { type: "result", result: { type: "artifact", uri: "artifact\ud800", hash: "hash\0" } },
      ].entries())
        yield* append({
          ...wait,
          waitId: `other-${index}`,
          target,
          managerThreadId: "manager-2",
          managerGeneration: 2,
        });
      const read = (yield* store.readWait({ taskId: "task", waitId: wait.waitId }))!;
      assert.deepStrictEqual(read.source.recorded, latest);
      assert.deepStrictEqual<unknown>(read.target, wait.target);
      assert.equal(read.managerGeneration, 2);
      assert.equal(read.state, "registered");
      const after = (yield* store.readFoundation("task"))!;
      for (const key of [
        "definitionRevision",
        "lifecycleRevision",
        "completionPolicyRevision",
        "actionPolicyRevision",
      ] as const)
        assert.equal(after.projection[key], current.projection[key]);
      assert.deepStrictEqual(after.projection.definition, current.projection.definition);
      assert.deepStrictEqual(
        after.projection.current?.configuration,
        current.projection.current?.configuration,
      );
      assert.deepStrictEqual<unknown>(after.draft, draft);
      let pure: ReturnType<typeof reduceTaskFoundation> | undefined;
      for (const value of history) {
        const event = value.event;
        switch (event.type) {
          case "task.created":
          case "task.definition-updated":
          case "task.configuration-updated":
          case "task.execution-updated":
          case "task.wait-updated":
            pure = reduceTaskFoundation(pure, { event, context: value.context });
        }
      }
      assert.deepStrictEqual(HashMap.getUnsafe(pure!.waits, wait.waitId), latest);
      const snapshot = yield* wakeSnapshot();
      for (const value of history.filter((value) => value.event.type === "task.wait-updated"))
        assert.isFalse((yield* store.applyWait(value)).applied);
      assert.deepStrictEqual(yield* wakeSnapshot(), snapshot);
      assert.deepStrictEqual(
        yield* store
          .readWait({ taskId: "task", waitId: wait.waitId })
          .pipe(Effect.provide(TaskStore.layer)),
        read,
      );
      assert.isUndefined(yield* store.readWait({ taskId: "unused", waitId: wait.waitId }));
      const live = {
        foundation: after,
        wait: read,
        custody: yield* store.readManagerCustody("task"),
        allowance: yield* store.readAllowance("task"),
        wake: yield* store.readWake("task"),
      };
      const rebuilt = yield* Effect.gen(function* () {
        yield* runMigrations();
        const database = yield* SqlClient.SqlClient;
        const target = yield* TaskStore.TaskStore;
        assert.deepStrictEqual(yield* database`SELECT * FROM orchestration_events`, []);
        for (const [version, value] of history.entries()) yield* insert(value, version);
        const native = yield* database`SELECT * FROM orchestration_events ORDER BY sequence`;
        yield* target.rebuildCustodyProjection({
          taskId: "task",
          throughSequence: history.at(-1)!.context.sequence,
        });
        assert.deepStrictEqual(
          yield* database`SELECT * FROM orchestration_events ORDER BY sequence`,
          native,
        );
        return {
          foundation: yield* target.readFoundation("task"),
          wait: yield* target.readWait({ taskId: "task", waitId: wait.waitId }),
          custody: yield* target.readManagerCustody("task"),
          allowance: yield* target.readAllowance("task"),
          wake: yield* target.readWake("task"),
        };
      }).pipe(Effect.provide(Layer.fresh(testLayer)));
      assert.deepStrictEqual(rebuilt, live);
      assert.equal(original.event.type, "task.wait-updated");
    }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects contradictory wait evidence and rolls back common publication atomically", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seedWakeCustody();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const wait = {
      type: "task.wait-updated",
      waitId: "wait",
      managerThreadId: "manager",
      managerGeneration: 1,
      target: { type: "source", sourceId: "source" },
      state: "registered",
    };
    const append = (input: Record<string, unknown>) =>
      appendTaskFact(input).pipe(sql.withTransaction);
    const reject = Effect.fn("TaskStoreTest.rejectWait")(function* (
      input: Record<string, unknown>,
    ) {
      const before = yield* wakeSnapshot();
      assert.equal((yield* Effect.exit(append(input)))._tag, "Failure");
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
    });
    yield* reject({ ...wait, state: "cleared" });
    yield* reject({ ...wait, managerThreadId: "wrong" });
    yield* reject({ ...wait, managerGeneration: 2 });
    const first = yield* append(wait);
    yield* reject({ ...wait, state: "cleared", target: { type: "source", sourceId: "other" } });
    for (const table of [
      "work_task_waits",
      "work_task_current_state",
      "work_task_manager_reference_checkpoints",
      "work_task_view_fence",
    ]) {
      yield* sql.unsafe(
        `CREATE TEMP TRIGGER fail_wait_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(IGNORE); END`,
      );
      yield* reject({ ...wait, state: "cleared" });
      yield* sql`DROP TRIGGER fail_wait_update`;
    }
    const before = yield* wakeSnapshot();
    assert.equal(
      (yield* Effect.exit(
        Effect.gen(function* () {
          yield* appendTaskFact({ ...wait, state: "cleared" });
          yield* appendTaskFact({ ...wait, target: { type: "source", sourceId: "changed" } });
        }).pipe(sql.withTransaction),
      ))._tag,
      "Failure",
    );
    assert.deepStrictEqual(yield* wakeSnapshot(), before);
    yield* append({ ...wait, state: "cleared" });
    yield* append(wait);
    yield* append(wait);
    assert.equal((yield* store.readWait({ taskId: "task", waitId: "wait" }))?.state, "registered");
    assert.isFalse((yield* store.applyWait(first)).applied);
    const revisions = {
      definitionRevision: 2,
      lifecycleRevision: 2,
      completionPolicyRevision: 2,
      actionPolicyRevision: 2,
    };
    yield* append({
      type: "task.definition-updated",
      ...revisions,
      definition: { ...definition, revision: 2 },
    });
    yield* reject(configuration);
    yield* append({ ...wait, state: "cleared" });
    const [head] = yield* sql<{
      sequence: number;
      version: number;
    }>`SELECT max(sequence)+1 AS sequence,max(stream_version)+1 AS version FROM orchestration_events`;
    const pending = yield* record(head!.sequence, wait);
    yield* insert(pending, head!.version);
    const applied = yield* Effect.all([store.applyWait(pending), store.applyWait(pending)], {
      concurrency: 2,
    });
    assert.equal(applied.filter((value) => value.applied).length, 1);
    assert.equal(
      (yield* store.readWait({ taskId: "task", waitId: "wait" }))?.updatedSequence,
      pending.context.sequence,
    );
    for (const query of [
      "SELECT * FROM work_task_waits WHERE task_id='task' AND wait_id='wait'",
      "SELECT 1 FROM work_task_waits WHERE task_id='task' LIMIT 1",
      "UPDATE work_task_waits SET state='cleared' WHERE task_id='task' AND wait_id='wait' AND updated_sequence=5 AND state='registered' AND manager_generation=1 AND hex(target_json)='00'",
    ]) {
      const plans = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(plans.some((row) => row.detail.includes("SEARCH work_task_waits")));
      assert.isFalse(plans.some((row) => /SCAN work_task_waits|TEMP B-TREE/.test(row.detail)));
    }
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "fails closed on missing or unproven wait materialization and future manager births",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      yield* seedWakeCustody();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const wait = {
        type: "task.wait-updated",
        waitId: "wait",
        managerThreadId: "manager",
        managerGeneration: 1,
        target: { type: "source", sourceId: "source" },
        state: "registered",
      };
      const own = yield* appendTaskFact(wait).pipe(sql.withTransaction);
      const read = store.readWait({ taskId: "task", waitId: "wait" });
      const saved = yield* read;
      const broken = Effect.fn("TaskStoreTest.rejectWaitMaterialization")(function* (
        mutation: Effect.Effect<unknown, SqlError>,
      ) {
        let observed: string | undefined;
        yield* Effect.gen(function* () {
          yield* mutation.pipe(Effect.orDie);
          observed = (yield* Effect.exit(read))._tag;
          return yield* Effect.fail("restore fixture");
        }).pipe(sql.withTransaction, Effect.ignore);
        assert.equal(observed, "Failure");
        assert.deepStrictEqual(yield* read, saved);
      });
      yield* broken(sql`DELETE FROM work_task_waits WHERE task_id='task'`);
      yield* broken(
        sql`UPDATE work_task_waits SET target_json='{"type":"source","sourceId":"changed"}' WHERE task_id='task'`,
      );
      yield* broken(sql`UPDATE work_task_waits SET updated_sequence=4 WHERE task_id='task'`);
      yield* broken(sql`UPDATE work_task_waits SET state='cleared' WHERE task_id='task'`);
      yield* broken(sql`DELETE FROM orchestration_events WHERE event_id=${own.event.id}`);
      yield* broken(sql`DELETE FROM work_task_current_state WHERE task_id='task'`);
      yield* broken(
        Effect.gen(function* () {
          yield* sql`DROP TRIGGER work_task_reference_delete`;
          yield* sql`DELETE FROM work_task_manager_reference_checkpoints WHERE task_id='task'`;
        }),
      );
      const nativeOnly = yield* record(own.context.sequence + 1, {
        ...wait,
        taskId: "native-only",
      });
      yield* insert(nativeOnly, 0);
      assert.equal(
        (yield* Effect.exit(store.readWait({ taskId: "native-only", waitId: "wait" })))._tag,
        "Failure",
      );
      assert.isUndefined(yield* store.readWait({ taskId: "unused", waitId: "wait" }));
      yield* Effect.gen(function* () {
        yield* runMigrations();
        const target = yield* TaskStore.TaskStore;
        const database = yield* SqlClient.SqlClient;
        const created = yield* record(1);
        const early = yield* record(2, wait);
        const birth = yield* record(3, {
          type: "task.manager-bound",
          managerThreadId: "manager",
          managerGeneration: 1,
        });
        for (const [version, value] of [created, early, birth].entries())
          yield* insert(value, version);
        const before = yield* wakeSnapshot();
        assert.equal(
          (yield* Effect.exit(
            target.rebuildCustodyProjection({ taskId: "task", throughSequence: 3 }),
          ))._tag,
          "Failure",
        );
        assert.deepStrictEqual(yield* wakeSnapshot(), before);
        yield* target.applyFoundation(created);
        yield* database`UPDATE work_task_current_state SET last_applied_sequence=3 WHERE task_id='task'`;
        yield* database`UPDATE work_task_manager_reference_checkpoints SET checked_event_id='event-3',checked_sequence=3,checked_stream_version=2 WHERE task_id='task'`;
        yield* database`INSERT INTO work_task_manager_bindings VALUES('task',1,'manager',${timestamp},NULL)`;
        yield* database`INSERT INTO work_task_waits VALUES('task','wait',1,${JSON.stringify(wait.target)},'registered',2,${timestamp})`;
        assert.equal(
          (yield* target.readManagerBinding({ taskId: "task", generation: 1 }))?.bound.recorded
            .context.sequence,
          3,
        );
        const failure = yield* Effect.flip(target.readWait({ taskId: "task", waitId: "wait" }));
        assert.equal(failure._tag, "TaskProjectionError");
        if (failure._tag === "TaskProjectionError") assert.equal(failure.code, "manager");
        assert.equal(
          (yield* Effect.exit(
            target.rebuildCustodyProjection({ taskId: "task", throughSequence: 3 }),
          ))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(Layer.fresh(testLayer)));
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "applies native wake facts exactly once and rebuilds the complete supported custody prefix",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const history = [yield* record(1), ...(yield* seedWakeCustody())];
      const append = Effect.fn("TaskStoreTest.appendWakeHistory")(function* (
        input: Record<string, unknown>,
      ) {
        const value = yield* appendTaskFact({
          definitionRevision: history.at(-1)!.event.definitionRevision,
          ...input,
        }).pipe(sql.withTransaction);
        history.push(value);
        return value;
      });
      yield* append(humanAcceptance);
      yield* append(configuration);
      yield* append({
        ...execution(1),
        manager: { managerThreadId: "manager", managerGeneration: 1 },
      });
      const linked = yield* append({ type: "task.thread-linked", threadId: "ordinary" });
      yield* append({ type: "task.thread-unlinked", threadId: "ordinary" });
      const draft = { ...definition, revision: 2, title: "Mixed history draft" };
      yield* append({ type: "task.definition-updated", definitionRevision: 2, definition: draft });
      yield* append({ ...humanAcceptance, definition: draft });
      yield* append({
        type: "task.allowance-topped-up",
        attribution: { type: "user", subject: "human", sessionId: "session" },
        previous: { type: "configured", cap: 2, consumed: 1 },
        resulting: { type: "configured", cap: 3, consumed: 1 },
      });
      const sources = Array.from({ length: 33 }, (_, index) => ({
        kind: "result",
        sourceId: `source-${index + 1}`,
        resultRef: null,
      }));
      for (const [index, source] of sources.entries())
        yield* append({
          type: "task.wake-source-recorded",
          source,
          reservationId: null,
          dirtyGeneration: index + 1,
        });
      yield* append({
        type: "task.wake-source-recorded",
        source: sources[0],
        reservationId: null,
        dirtyGeneration: 1,
      });
      const base = {
        reservationId: "reservation",
        reservationCommandId: "reserve-command",
        managerGeneration: 1,
        allowanceUnit: 1,
        dirtyGeneration: 33,
        acknowledgedGeneration: 0,
      };
      const pending = { sources: sources.slice(0, 32), overflowThroughGeneration: 33 };
      yield* append({ type: "task.wake-reserved", ...base, pending });
      yield* append({ type: "task.wake-claim-uncertain", ...base, reason: "lost response" });
      yield* append({
        type: "task.manager-retired",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      const first = yield* append({
        type: "task.wake-claimed",
        ...base,
        pending,
        messageId: "message",
        runId: "run",
        occurredAt: "2026-11-16T00:00:00.0001Z",
      });
      yield* append({
        type: "task.wake-source-recorded",
        source: { kind: "result", sourceId: "source-34", resultRef: null },
        reservationId: "reservation",
        dirtyGeneration: 34,
      });
      const refresh = yield* append({
        type: "task.wake-claimed",
        ...base,
        dirtyGeneration: 34,
        pending: { ...pending, overflowThroughGeneration: 34 },
        messageId: "message",
        runId: "run",
      });
      yield* append({
        type: "task.wake-acknowledged",
        ...base,
        dirtyGeneration: 34,
        previousAcknowledgedGeneration: 0,
        acknowledgedGeneration: 16,
        observedDirtyGeneration: 34,
      });
      const beforeLast = yield* store.readWake("task");
      assert.equal(beforeLast?.pending.length, 16);
      yield* append({
        type: "task.wake-acknowledged",
        ...base,
        dirtyGeneration: 34,
        previousAcknowledgedGeneration: 16,
        acknowledgedGeneration: 34,
        observedDirtyGeneration: 34,
      });
      assert.sameMembers(
        [...new Set(history.map((value) => value.event.type))],
        [
          "task.created",
          "task.definition-updated",
          "task.definition-accepted",
          "task.configuration-updated",
          "task.execution-updated",
          "task.thread-linked",
          "task.thread-unlinked",
          "task.manager-bound",
          "task.manager-retired",
          "task.allowance-configured",
          "task.allowance-topped-up",
          "task.allowance-debited",
          "task.wake-source-recorded",
          "task.wake-reserved",
          "task.wake-claimed",
          "task.wake-claim-uncertain",
          "task.wake-acknowledged",
        ],
      );
      const before = yield* wakeSnapshot();
      for (const value of history.filter((value) => value.event.type.startsWith("task.wake-")))
        assert.isFalse((yield* store.applyWake(value)).applied);
      assert.deepStrictEqual(yield* wakeSnapshot(), before);
      const view = (yield* store.readWake("task"))!;
      assert.equal(view.dirtyGeneration, 34);
      assert.equal(view.acknowledgedGeneration, 34);
      assert.isNull(view.overflowThroughGeneration);
      assert.deepStrictEqual(view.pending, []);
      assert.equal((yield* store.readAllowance("task"))?.allowance.consumed, 1);
      const reservation = (yield* store.readWakeReservation({
        taskId: "task",
        reservationId: "reservation",
      }))!;
      assert.deepStrictEqual(reservation.firstClaim?.recorded, first);
      assert.deepStrictEqual(reservation.latestClaim?.recorded, refresh);
      let pure: ReturnType<typeof reduceTaskCustody> | undefined;
      for (const value of history) {
        const event = value.event;
        switch (event.type) {
          case "task.manager-bound":
          case "task.manager-retired":
          case "task.allowance-topped-up":
          case "task.allowance-configured":
          case "task.allowance-debited":
          case "task.wake-source-recorded":
          case "task.wake-reserved":
          case "task.wake-claimed":
          case "task.wake-claim-uncertain":
          case "task.wake-acknowledged":
            pure = reduceTaskCustody(pure, { event, context: value.context });
        }
      }
      assert.equal(view.dirtyGeneration, pure?.dirtyGeneration);
      assert.equal(view.acknowledgedGeneration, pure?.acknowledgedGeneration);
      assert.equal(view.overflowThroughGeneration, pure?.overflowThroughGeneration);
      let foundation: ReturnType<typeof reduceTaskFoundation> | undefined;
      for (const value of history) {
        const event = value.event;
        switch (event.type) {
          case "task.created":
          case "task.definition-updated":
          case "task.definition-accepted":
          case "task.configuration-updated":
          case "task.execution-updated":
          case "task.thread-linked":
          case "task.thread-unlinked":
            foundation = reduceTaskFoundation(foundation, { event, context: value.context });
        }
      }
      assert.deepStrictEqual(
        (yield* store.readFoundation("task"))?.draft,
        foundation!.draft.event.definition,
      );
      assert.deepStrictEqual((yield* store.readAllowance("task"))?.allowance, {
        type: "configured",
        cap: 3,
        consumed: 1,
      });
      const live = {
        foundation: yield* store.readFoundation("task"),
        link: yield* store.readThreadLink({ taskId: "task", linkEventId: linked.event.id }),
        custody: yield* store.readManagerCustody("task"),
        allowance: yield* store.readAllowance("task"),
        view,
        reservation,
      };
      const replayed = yield* Effect.gen(function* () {
        yield* runMigrations();
        const rebuilt = yield* TaskStore.TaskStore;
        const database = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* database`SELECT * FROM orchestration_events`, []);
        assert.deepStrictEqual(yield* database`SELECT * FROM work_task_projections`, []);
        for (const [version, value] of history.entries()) yield* insert(value, version);
        const sourceBefore = yield* database`SELECT * FROM orchestration_events ORDER BY sequence`;
        yield* rebuilt.rebuildCustodyProjection({
          taskId: "task",
          throughSequence: history.at(-1)!.context.sequence,
        });
        assert.deepStrictEqual(
          yield* database`SELECT * FROM orchestration_events ORDER BY sequence`,
          sourceBefore,
        );
        return {
          foundation: yield* rebuilt.readFoundation("task"),
          link: yield* rebuilt.readThreadLink({ taskId: "task", linkEventId: linked.event.id }),
          custody: yield* rebuilt.readManagerCustody("task"),
          allowance: yield* rebuilt.readAllowance("task"),
          view: yield* rebuilt.readWake("task"),
          reservation: yield* rebuilt.readWakeReservation({
            taskId: "task",
            reservationId: "reservation",
          }),
        };
      }).pipe(Effect.provide(Layer.fresh(testLayer)));
      assert.deepStrictEqual(replayed, live);
      assert.deepStrictEqual(
        yield* store.readWake("task").pipe(Effect.provide(TaskStore.layer)),
        view,
      );
      yield* Effect.gen(function* () {
        yield* runMigrations();
        const rebuilt = yield* TaskStore.TaskStore;
        for (const [version, value] of history.entries()) yield* insert(value, version);
        const unsupported = yield* record(history.at(-1)!.context.sequence + 1, {
          type: "task.archived",
          archived: true,
        });
        yield* insert(unsupported, history.length);
        const clean = yield* wakeSnapshot();
        assert.equal(
          (yield* Effect.exit(
            rebuilt.rebuildCustodyProjection({
              taskId: "task",
              throughSequence: unsupported.context.sequence,
            }),
          ))._tag,
          "Failure",
        );
        assert.deepStrictEqual(yield* wakeSnapshot(), clean);
      }).pipe(Effect.provide(Layer.fresh(testLayer)));
    }).pipe(Effect.provide(testLayer)),
);
it.effect(
  "rolls back wake phase, allowance and source effects on conflicts and failed commits",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      yield* seedWakeCustody();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const append = (input: Record<string, unknown>) =>
        appendTaskFact(input).pipe(sql.withTransaction);
      const reject = Effect.fn("TaskStoreTest.rejectWakeWrite")(function* (
        input: Record<string, unknown>,
      ) {
        const before = yield* wakeSnapshot();
        assert.equal((yield* Effect.exit(append(input)))._tag, "Failure");
        assert.deepStrictEqual(yield* wakeSnapshot(), before);
      });
      const source = { kind: "result\u0000\ud800", sourceId: "one\ud800", resultRef: null };
      yield* append({
        type: "task.wake-source-recorded",
        source,
        reservationId: null,
        dirtyGeneration: 1,
      });
      const base = {
        reservationId: "r\ud800",
        reservationCommandId: "reserved\ud800",
        managerGeneration: 1,
        allowanceUnit: 1,
        dirtyGeneration: 1,
        acknowledgedGeneration: 0,
      };
      const reserved = {
        type: "task.wake-reserved",
        ...base,
        pending: { sources: [source], overflowThroughGeneration: null },
      };
      yield* reject({
        type: "task.wake-source-recorded",
        source: { ...source, resultRef: "changed" },
        reservationId: null,
        dirtyGeneration: 1,
      });
      yield* reject({
        type: "task.wake-source-recorded",
        source,
        reservationId: null,
        dirtyGeneration: 2,
      });
      yield* reject({ ...reserved, allowanceUnit: 2 });
      yield* reject({ ...reserved, managerGeneration: 2 });
      yield* reject({ ...reserved, pending: { sources: [], overflowThroughGeneration: null } });
      yield* reject({
        ...reserved,
        pending: { sources: [{ ...source, sourceId: "unknown" }], overflowThroughGeneration: null },
      });
      yield* reject({ ...reserved, pending: { sources: [source], overflowThroughGeneration: 1 } });
      yield* append(reserved);
      yield* reject({ ...reserved, reservationId: "other" });
      yield* reject({ ...reserved, reservationCommandId: "other" });
      const uncertain = { type: "task.wake-claim-uncertain", ...base, reason: "lost" };
      yield* append(uncertain);
      yield* reject(uncertain);
      const claim = {
        ...reserved,
        type: "task.wake-claimed",
        messageId: "message\ud800",
        runId: "run\ud800",
      };
      yield* reject({ ...claim, reservationCommandId: "different" });
      const first = yield* append(claim);
      yield* reject(claim);
      const second = { kind: "result", sourceId: "two", resultRef: null };
      yield* append({
        type: "task.wake-source-recorded",
        source: second,
        reservationId: base.reservationId,
        dirtyGeneration: 2,
      });
      const refresh = {
        ...claim,
        dirtyGeneration: 2,
        pending: { sources: [source, second], overflowThroughGeneration: null },
      };
      yield* reject({ ...refresh, messageId: "different" });
      yield* append(refresh);
      const acknowledgement = {
        type: "task.wake-acknowledged",
        ...base,
        dirtyGeneration: 2,
        previousAcknowledgedGeneration: 0,
        acknowledgedGeneration: 1,
        observedDirtyGeneration: 2,
      };
      yield* reject({ ...acknowledgement, previousAcknowledgedGeneration: 1 });
      yield* reject({ ...acknowledgement, acknowledgedGeneration: 0 });
      yield* reject({ ...acknowledgement, observedDirtyGeneration: 1 });
      yield* append(acknowledgement);
      yield* reject({ ...refresh, acknowledgedGeneration: 1 });
      yield* reject({ ...uncertain, dirtyGeneration: 2, acknowledgedGeneration: 1 });
      const third = { kind: "result", sourceId: "three", resultRef: null };
      yield* append({
        type: "task.wake-source-recorded",
        source: third,
        reservationId: null,
        dirtyGeneration: 3,
      });
      yield* append({
        ...acknowledgement,
        dirtyGeneration: 3,
        previousAcknowledgedGeneration: 1,
        acknowledgedGeneration: 2,
      });
      yield* reject({
        ...acknowledgement,
        dirtyGeneration: 3,
        previousAcknowledgedGeneration: 2,
        acknowledgedGeneration: 2,
      });
      assert.equal(
        (yield* store.readWakeReservation({ taskId: "task", reservationId: base.reservationId }))
          ?.firstClaim?.recorded.event.id,
        first.event.id,
      );
      yield* append({
        type: "task.manager-retired",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      yield* append({
        type: "task.manager-bound",
        managerThreadId: "new-manager",
        managerGeneration: 2,
      });
      const debit = {
        type: "task.allowance-debited",
        previous: { type: "configured", cap: 2, consumed: 1 },
        resulting: { type: "configured", cap: 2, consumed: 2 },
        allowanceUnit: 2,
      };
      const nextReservation = {
        type: "task.wake-reserved",
        reservationId: "next",
        reservationCommandId: "next-command",
        managerGeneration: 2,
        allowanceUnit: 2,
        dirtyGeneration: 3,
        acknowledgedGeneration: 2,
        pending: { sources: [third], overflowThroughGeneration: null },
      };
      const beforeDebit = yield* wakeSnapshot();
      assert.equal(
        (yield* Effect.exit(
          Effect.gen(function* () {
            yield* appendTaskFact(debit);
            yield* appendTaskFact({
              ...nextReservation,
              pending: { sources: [], overflowThroughGeneration: null },
            });
          }).pipe(sql.withTransaction),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* wakeSnapshot(), beforeDebit);
      yield* sql`CREATE TABLE wake_commit_parent(id INTEGER PRIMARY KEY)`;
      yield* sql`CREATE TABLE wake_commit_child(id INTEGER REFERENCES wake_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`;
      assert.equal(
        (yield* Effect.exit(
          Effect.gen(function* () {
            yield* appendTaskFact(debit);
            yield* appendTaskFact(nextReservation);
            yield* sql`INSERT INTO wake_commit_child VALUES(1)`;
          }).pipe(sql.withTransaction),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* wakeSnapshot(), beforeDebit);
      assert.deepStrictEqual(yield* sql`SELECT * FROM wake_commit_child`, []);
      yield* Effect.gen(function* () {
        yield* appendTaskFact(debit);
        yield* appendTaskFact(nextReservation);
      }).pipe(sql.withTransaction);
      assert.equal((yield* store.readAllowance("task"))?.allowance.consumed, 2);
      const fourth = { kind: "result", sourceId: "four", resultRef: null };
      const sourceFour = {
        type: "task.wake-source-recorded",
        source: fourth,
        reservationId: null,
        dirtyGeneration: 4,
      };
      for (const table of [
        "work_task_current_state",
        "work_task_manager_reference_checkpoints",
        "work_task_view_fence",
      ]) {
        yield* sql.unsafe(
          `CREATE TRIGGER reject_wake_common BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'injected wake failure'); END`,
        );
        yield* reject(sourceFour);
        yield* sql`DROP TRIGGER reject_wake_common`;
      }
      const beforePhase = yield* wakeSnapshot();
      assert.equal(
        (yield* Effect.exit(
          Effect.gen(function* () {
            yield* appendTaskFact(sourceFour);
            yield* appendTaskFact({
              ...nextReservation,
              type: "task.wake-claimed",
              reservationCommandId: "wrong",
              dirtyGeneration: 4,
              pending: { sources: [third, fourth], overflowThroughGeneration: null },
              messageId: "next-message",
              runId: "next-run",
            });
          }).pipe(sql.withTransaction),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* wakeSnapshot(), beforePhase);
      yield* append(sourceFour);
      yield* append({
        ...nextReservation,
        type: "task.wake-claimed",
        dirtyGeneration: 4,
        pending: { sources: [third, fourth], overflowThroughGeneration: null },
        messageId: "next-message",
        runId: "next-run",
      });
      const [head] = yield* sql<{
        sequence: number;
        version: number;
      }>`SELECT max(sequence)+1 AS sequence,max(stream_version)+1 AS version FROM orchestration_events`;
      const fifth = yield* record(head!.sequence, {
        type: "task.wake-source-recorded",
        commandId: `command-${head!.sequence}`,
        source: { kind: "result", sourceId: "five", resultRef: null },
        reservationId: null,
        dirtyGeneration: 5,
      });
      yield* insert(fifth, head!.version);
      const outcomes = yield* Effect.all([store.applyWake(fifth), store.applyWake(fifth)], {
        concurrency: "unbounded",
      });
      assert.equal(outcomes.filter((value) => value.applied).length, 1);
      assert.equal((yield* store.readWake("task"))?.dirtyGeneration, 5);
      assert.equal((yield* store.readAllowance("task"))?.allowance.consumed, 2);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(testLayer)),
);
it.effect("rejects validation-owned allowance units before creating a wake reservation", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seedWakeCustody();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    yield* appendTaskFact({
      type: "task.definition-accepted",
      definition,
      attribution: { type: "user", subject: "owner", sessionId: "human" },
    }).pipe(sql.withTransaction);
    yield* sql`INSERT INTO work_task_claims VALUES('legacy-claim','task','claim','evidence','manager',1,1,1,0,'{"type":"server"}','summary','[{"type":"artifact","uri":"u","hash":"h"}]',1,${timestamp})`;
    yield* sql`INSERT INTO work_task_validation_attempts VALUES('legacy-attempt','task',1,1,1,'evidence','watermark','{}',NULL,'reserved',NULL,${timestamp},NULL,NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES('legacy-attempt','task','claim',1)`;
    const source = { kind: "result", sourceId: "one", resultRef: null };
    yield* appendTaskFact({
      type: "task.wake-source-recorded",
      source,
      reservationId: null,
      dirtyGeneration: 1,
    }).pipe(sql.withTransaction);
    const before = yield* wakeSnapshot();
    const outcome = yield* Effect.exit(
      appendTaskFact({
        type: "task.wake-reserved",
        reservationId: "r",
        reservationCommandId: "r-command",
        managerGeneration: 1,
        allowanceUnit: 1,
        dirtyGeneration: 1,
        acknowledgedGeneration: 0,
        pending: { sources: [source], overflowThroughGeneration: null },
      }).pipe(sql.withTransaction),
    );
    assert.equal(outcome._tag, "Failure");
    assert.deepStrictEqual(yield* wakeSnapshot(), before);
    assert.equal((yield* store.readAllowance("task"))?.allowance.consumed, 1);
    const plan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT 1 FROM work_task_validation_identity WHERE task_id='task' AND allowance_unit=1 LIMIT 1`;
    assert.isTrue(plan.some((row) => row.detail.includes("SEARCH")));
    assert.isFalse(plan.some((row) => /SCAN|TEMP B-TREE/.test(row.detail)));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("requires the exact historical phase companion instead of only a later claim", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seedWakeCustody();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const one = { kind: "result", sourceId: "one", resultRef: null };
    const two = { kind: "result", sourceId: "two", resultRef: null };
    const three = { kind: "result", sourceId: "three", resultRef: null };
    const append = (input: Record<string, unknown>) =>
      appendTaskFact(input).pipe(sql.withTransaction);
    yield* append({
      type: "task.wake-source-recorded",
      source: one,
      reservationId: null,
      dirtyGeneration: 1,
    });
    const base = {
      reservationId: "r",
      reservationCommandId: "reserve",
      managerGeneration: 1,
      allowanceUnit: 1,
      dirtyGeneration: 1,
      acknowledgedGeneration: 0,
      pending: { sources: [one], overflowThroughGeneration: null },
    };
    yield* append({ type: "task.wake-reserved", ...base });
    const first = yield* append({
      type: "task.wake-claimed",
      ...base,
      messageId: "m",
      runId: "run",
    });
    yield* append({
      type: "task.wake-source-recorded",
      source: two,
      reservationId: null,
      dirtyGeneration: 2,
    });
    const missing = yield* record(9, {
      type: "task.wake-claimed",
      commandId: "command-9",
      ...base,
      dirtyGeneration: 2,
      pending: { sources: [one, two], overflowThroughGeneration: null },
      messageId: "m",
      runId: "run",
    });
    const source = yield* record(10, {
      type: "task.wake-source-recorded",
      commandId: "command-10",
      source: three,
      reservationId: null,
      dirtyGeneration: 3,
    });
    const last = yield* record(11, {
      type: "task.wake-claimed",
      commandId: "command-11",
      ...base,
      dirtyGeneration: 3,
      pending: { sources: [one, two, three], overflowThroughGeneration: null },
      messageId: "m",
      runId: "run",
    });
    for (const [index, value] of [missing, source, last].entries()) yield* insert(value, index + 8);
    // Model an incomplete historical materialization with guards intact; only the current tail is complete.
    yield* sql`INSERT INTO work_task_wake_sources VALUES('task','result','three',NULL,NULL)`;
    yield* sql`INSERT INTO work_task_wake_source_state VALUES('task','result','three','event-10',10,3,1,NULL,NULL)`;
    yield* sql`INSERT INTO work_task_wake_phase_state VALUES('event-11',11,'task','r','claimed',3,0,NULL,NULL,'m','run',${timestamp})`;
    yield* sql`UPDATE work_task_current_state SET last_applied_sequence=11 WHERE task_id='task'`;
    assert.equal((yield* store.readWake("task"))?.dirtyGeneration, 3);
    const before = yield* wakeSnapshot();
    assert.equal((yield* Effect.exit(store.applyWake(missing)))._tag, "Failure");
    assert.isFalse((yield* store.applyWake(first)).applied);
    assert.deepStrictEqual(yield* wakeSnapshot(), before);
    const plan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT * FROM work_task_wake_phase_state WHERE event_id='event-7' AND task_id='task' AND reservation_id='r' AND kind='claimed' ORDER BY sequence DESC LIMIT 1`;
    assert.isTrue(plan.some((row) => row.detail.includes("SEARCH")));
    assert.isFalse(plan.some((row) => /SCAN|TEMP B-TREE/.test(row.detail)));
  }).pipe(Effect.provide(testLayer)),
);
it.effect(
  "reads native wake arrivals, original claims and incremental observations without writes",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const fixture = yield* wakeFixture();
      const sources = Array.from({ length: 33 }, (_, index) => ({
        kind: "custom",
        sourceId: `source-${index + 1}`,
        resultRef: null,
      }));
      for (const [index, source] of sources.entries())
        yield* fixture.append({
          type: "task.wake-source-recorded",
          source,
          reservationId: null,
          dirtyGeneration: index + 1,
        });
      const original = yield* store.readWakeSource({
        taskId: "task",
        kind: "custom",
        sourceId: "source-1",
      });
      const repeated = yield* fixture.append({
        type: "task.wake-source-recorded",
        source: sources[0],
        reservationId: null,
        dirtyGeneration: 1,
      });
      assert.notEqual(original?.eventId, repeated.event.id);
      assert.equal(
        (yield* store.readWakeSource({ taskId: "task", kind: "custom", sourceId: "source-1" }))
          ?.eventId,
        original?.eventId,
      );
      const base = {
        reservationId: "reservation",
        reservationCommandId: "reservation-command",
        managerGeneration: 1,
        allowanceUnit: 1,
        dirtyGeneration: 33,
        acknowledgedGeneration: 0,
      };
      const pending = { sources: sources.slice(0, 32), overflowThroughGeneration: 33 };
      yield* fixture.append({ type: "task.wake-reserved", ...base, pending });
      yield* fixture.append({
        type: "task.wake-claim-uncertain",
        ...base,
        reason: "response lost",
      });
      const first = yield* fixture.append({
        type: "task.wake-claimed",
        ...base,
        pending,
        messageId: "message",
        runId: "run",
        occurredAt: "2026-11-16T00:00:00.0001Z",
      });
      yield* fixture.append({
        type: "task.wake-source-recorded",
        source: { kind: "custom", sourceId: "source-34", resultRef: null },
        reservationId: null,
        dirtyGeneration: 34,
      });
      const refresh = yield* fixture.append({
        type: "task.wake-claimed",
        ...base,
        dirtyGeneration: 34,
        pending: { ...pending, overflowThroughGeneration: 34 },
        messageId: "message",
        runId: "run",
        occurredAt: "2026-11-16T00:00:00.0009Z",
      });
      yield* fixture.append({
        type: "task.wake-acknowledged",
        ...base,
        dirtyGeneration: 34,
        previousAcknowledgedGeneration: 0,
        acknowledgedGeneration: 16,
        observedDirtyGeneration: 34,
      });
      const middle = (yield* store.readWake("task"))!;
      assert.equal(middle.pending.length, 16);
      assert.equal(middle.overflowThroughGeneration, 34);
      yield* fixture.append({
        type: "task.wake-acknowledged",
        ...base,
        dirtyGeneration: 34,
        previousAcknowledgedGeneration: 16,
        acknowledgedGeneration: 34,
        observedDirtyGeneration: 34,
      });
      const reservation = (yield* store.readWakeReservation({
        taskId: "task",
        reservationId: "reservation",
      }))!;
      assert.deepStrictEqual(reservation.firstClaim?.recorded, first);
      assert.deepStrictEqual(reservation.latestClaim?.recorded, refresh);
      assert.equal(reservation.claimedAt, first.event.occurredAt);
      assert.equal(reservation.uncertain?.recorded.event.type, "task.wake-claim-uncertain");
      assert.isUndefined(
        yield* store.readWakeReservation({ taskId: "other", reservationId: "reservation" }),
      );
      let pure: ReturnType<typeof reduceTaskCustody> | undefined;
      for (const value of fixture.recorded) {
        const event = value.event;
        switch (event.type) {
          case "task.manager-bound":
          case "task.allowance-configured":
          case "task.allowance-debited":
          case "task.wake-source-recorded":
          case "task.wake-reserved":
          case "task.wake-claimed":
          case "task.wake-claim-uncertain":
          case "task.wake-acknowledged":
            pure = reduceTaskCustody(pure, { event, context: value.context });
            break;
          default:
            assert.fail("unexpected fixture family");
        }
      }
      const view = (yield* store.readWake("task"))!;
      assert.equal(view.dirtyGeneration, 34);
      assert.equal(view.acknowledgedGeneration, 34);
      assert.deepStrictEqual(view.pending, []);
      assert.isNull(view.overflowThroughGeneration);
      assert.equal(view.dirtyGeneration, pure?.dirtyGeneration);
      assert.equal(view.acknowledgedGeneration, pure?.acknowledgedGeneration);
      assert.equal(view.overflowThroughGeneration, pure?.overflowThroughGeneration);
      const before = yield* sql`SELECT total_changes() AS changes`;
      assert.deepStrictEqual(yield* store.readWake("task"), view);
      assert.deepStrictEqual(
        yield* store.readWake("task").pipe(Effect.provide(TaskStore.layer)),
        view,
      );
      yield* store.readWakeSource({ taskId: "task", kind: "custom", sourceId: "source-33" });
      assert.deepStrictEqual(yield* sql`SELECT total_changes() AS changes`, before);
      const prior = (yield* sql<{
        last_applied_sequence: number;
      }>`SELECT last_applied_sequence FROM work_task_current_state WHERE task_id='task'`)[0]!
        .last_applied_sequence;
      const retired = yield* record(prior + 1, {
        type: "task.manager-retired",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      yield* insert(retired, prior);
      yield* store.auditManagerReferences({ taskId: "task", expectedCheckpointSequence: prior });
      yield* store.applyManagerAllowance(retired);
      assert.deepStrictEqual(
        yield* store.readWakeReservation({ taskId: "task", reservationId: "reservation" }),
        reservation,
      );
      for (const query of [
        "SELECT * FROM work_task_wake_source_state WHERE task_id='task' AND retained_pending=1 ORDER BY dirty_generation LIMIT 33",
        "SELECT * FROM work_task_wake_source_state WHERE task_id='task' AND dirty_generation=34 AND retained_pending=0",
        "SELECT * FROM work_task_wake_phase_state WHERE reservation_id='reservation' AND task_id='task' AND kind='claimed' ORDER BY sequence ASC LIMIT 1",
        "SELECT * FROM work_task_wake_phase_state WHERE reservation_id='reservation' AND task_id='task' AND kind='claimed' ORDER BY sequence DESC LIMIT 1",
        "SELECT * FROM work_task_wake_source_state WHERE task_id='task' AND retained_pending=0 AND dirty_generation>16 ORDER BY dirty_generation DESC LIMIT 1",
        "SELECT * FROM work_task_wake_phase_state WHERE task_id='task' AND kind='acknowledged' ORDER BY acknowledged_generation DESC LIMIT 1",
        "SELECT 1 FROM orchestration_events WHERE aggregate_kind='task' AND stream_id='task' LIMIT 1",
        "SELECT * FROM work_task_wake_state WHERE task_id='task'",
        "SELECT * FROM work_task_wake_source_state WHERE task_id='task' AND source_kind='custom' AND source_id='source-1'",
        "SELECT * FROM work_task_wake_sources WHERE task_id='task' AND source_kind='custom' AND source_id='source-1'",
        "SELECT * FROM work_task_wake_reservations WHERE reservation_id='reservation' AND task_id='task'",
        "SELECT source_sequence FROM work_task_wake_source_state WHERE task_id='task' ORDER BY source_sequence DESC LIMIT 1",
        "SELECT sequence FROM work_task_wake_phase_state WHERE task_id='task' ORDER BY sequence DESC LIMIT 1",
        "SELECT dirty_generation FROM work_task_wake_source_state WHERE task_id='task' ORDER BY dirty_generation DESC LIMIT 1",
      ]) {
        const plans = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
        assert.isTrue(plans.some((row) => row.detail.includes("SEARCH")));
        assert.isFalse(plans.some((row) => /SCAN|TEMP B-TREE/.test(row.detail)));
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects unavailable wake materialization and preserves exact source bytes", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const fixture = yield* wakeFixture();
    const source = { kind: "\u0000custom\ud800", sourceId: "id\ud800", resultRef: null };
    const first = yield* fixture.append({
      type: "task.wake-source-recorded",
      source,
      reservationId: null,
      dirtyGeneration: 1,
    });
    const actual = (yield* store.readWakeSource({
      taskId: "task",
      kind: source.kind,
      sourceId: source.sourceId,
    }))!;
    assert.equal(actual.kind, source.kind);
    assert.equal(actual.sourceId, source.sourceId);
    assert.deepStrictEqual(actual.source.recorded, first);
    yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,1,${timestamp})`;
    assert.equal((yield* Effect.exit(store.readWake("task")))._tag, "Failure");
    yield* sql`DELETE FROM orchestration_v2_projection_metadata WHERE projection_name='event-log-pruned-through'`;
    assert.deepStrictEqual(
      yield* store.readWakeSource({ taskId: "task", kind: source.kind, sourceId: source.sourceId }),
      actual,
    );
    const original = (yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_events WHERE sequence=${first.context.sequence}`)[0]!
      .payload_json;
    for (const changed of [
      { ...first.event, source: { ...source, resultRef: "changed" } },
      { ...first.event, dirtyGeneration: 2 },
      { ...first.event, reservationId: "absent" },
    ]) {
      yield* sql`UPDATE orchestration_events SET payload_json=${JSON.stringify(changed)} WHERE sequence=${first.context.sequence}`;
      assert.equal((yield* Effect.exit(store.readWake("task")))._tag, "Failure");
      assert.equal(
        (yield* Effect.exit(
          store.readWakeSource({ taskId: "task", kind: "other", sourceId: "absent" }),
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.exit(store.readWakeReservation({ taskId: "task", reservationId: "absent" })))
          ._tag,
        "Failure",
      );
      yield* sql`UPDATE orchestration_events SET payload_json=${original} WHERE sequence=${first.context.sequence}`;
    }
    const missing = yield* record(6, {
      type: "task.wake-source-recorded",
      commandId: "command-6",
      source: { kind: "other", sourceId: "missing", resultRef: null },
      reservationId: null,
      dirtyGeneration: 2,
    });
    yield* insert(missing, 5);
    yield* sql`UPDATE work_task_current_state SET last_applied_sequence=6 WHERE task_id='task'`;
    assert.equal((yield* Effect.exit(store.readWake("task")))._tag, "Failure");
    yield* sql`UPDATE work_task_current_state SET last_applied_sequence=5 WHERE task_id='task'`;
    yield* sql`INSERT INTO work_task_wake_reservations VALUES('legacy','task',1,'legacy-command','partial-message',NULL,2,1,0,${timestamp},NULL)`;
    assert.equal(
      (yield* Effect.exit(store.readWakeReservation({ taskId: "task", reservationId: "legacy" })))
        ._tag,
      "Failure",
    );
    const created = yield* record(7, { taskId: "isolated" });
    yield* insert(created, 0);
    yield* store.applyFoundation(created);
    assert.equal((yield* store.readWake("isolated"))?.dirtyGeneration, 0);
    assert.isUndefined(
      yield* store.readWakeSource({
        taskId: "isolated",
        kind: source.kind,
        sourceId: source.sourceId,
      }),
    );
    yield* sql`UPDATE orchestration_events SET event_id='wrong-own-id' WHERE sequence=5`;
    assert.equal((yield* Effect.exit(store.readWake("task")))._tag, "Failure");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("reads known zero wake state without hiding missing materialization", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const store = yield* TaskStore.TaskStore;
    assert.isUndefined(yield* store.readWake("unused"));
    const created = yield* record(1);
    yield* insert(created, 0);
    assert.isTrue((yield* Effect.exit(store.readWake("task")))._tag === "Failure");
    yield* store.applyFoundation(created);
    const wake = yield* store.readWake("task");
    assert.deepStrictEqual(wake, {
      taskId: created.event.taskId,
      checkpointSequence: 1,
      dirtyGeneration: 0,
      acknowledgedGeneration: 0,
      overflowThroughGeneration: null,
      pending: [],
    });
    assert.isUndefined(
      yield* store.readWakeSource({ taskId: "task", kind: "custom", sourceId: "absent" }),
    );
    assert.isUndefined(
      yield* store.readWakeReservation({ taskId: "task", reservationId: "absent" }),
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "persists manager custody and finite allowance independently of execution observations",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* TaskStore.TaskStore;
      const sql = yield* SqlClient.SqlClient;
      const created = yield* record(1);
      yield* insert(created, 0);
      yield* store.applyFoundation(created);
      assert.isUndefined(yield* store.readAllowance("missing"));
      assert.deepStrictEqual((yield* store.readAllowance("task"))?.allowance, {
        type: "unconfigured",
        consumed: 0,
      });
      const observed = yield* record(2, execution(1));
      yield* insert(observed, 1);
      yield* store.applyFoundation(observed);
      assert.isUndefined((yield* store.readManagerCustody("task"))?.current);
      const events = [
        { type: "task.manager-bound", managerThreadId: "manager-1", managerGeneration: 1 },
        {
          type: "task.allowance-configured",
          commandId: "command-4",
          previous: { type: "unconfigured", consumed: 0 },
          resulting: { type: "configured", cap: 2, consumed: 0 },
        },
        {
          type: "task.allowance-debited",
          commandId: "command-5",
          previous: { type: "configured", cap: 2, consumed: 0 },
          resulting: { type: "configured", cap: 2, consumed: 1 },
          allowanceUnit: 1,
        },
        {
          type: "task.allowance-topped-up",
          commandId: "command-6",
          attribution: { type: "user", subject: "owner", sessionId: "human" },
          previous: { type: "configured", cap: 2, consumed: 1 },
          resulting: { type: "configured", cap: 3, consumed: 1 },
        },
        {
          type: "task.allowance-configured",
          commandId: "command-7",
          previous: { type: "configured", cap: 3, consumed: 1 },
          resulting: { type: "configured", cap: 2, consumed: 1 },
        },
        {
          type: "task.allowance-debited",
          commandId: "command-8",
          previous: { type: "configured", cap: 2, consumed: 1 },
          resulting: { type: "configured", cap: 2, consumed: 2 },
          allowanceUnit: 2,
        },
        { type: "task.manager-retired", managerThreadId: "manager-1", managerGeneration: 1 },
        { type: "task.manager-bound", managerThreadId: "manager-2", managerGeneration: 3 },
      ];
      let pure: ReturnType<typeof reduceTaskCustody> | undefined;
      const applied: WorkTaskPersistedEvent[] = [];
      for (const [index, event] of events.entries()) {
        const value = yield* record(index + 3, event);
        yield* insert(value, index + 2);
        const typed = value.event;
        switch (typed.type) {
          case "task.manager-bound":
          case "task.manager-retired":
          case "task.allowance-configured":
          case "task.allowance-topped-up":
          case "task.allowance-debited":
            pure = reduceTaskCustody(pure, { event: typed, context: value.context });
        }
        assert.isTrue((yield* store.applyManagerAllowance(value)).applied);
        applied.push(value);
        const custody = yield* store.readManagerCustody("task");
        assert.strictEqual(custody?.highestGeneration, pure?.highestManagerGeneration);
        assert.strictEqual(custody?.current?.managerGeneration, pure?.currentManagerGeneration);
        assert.deepStrictEqual((yield* store.readAllowance("task"))?.allowance, pure?.allowance);
      }
      assert.strictEqual(
        (yield* store.readFoundation("task"))?.projection.current?.managerGeneration,
        1,
      );
      assert.strictEqual(
        (yield* store.readManagerBinding({ taskId: "task", generation: 1 }))?.retiredAt,
        timestamp,
      );
      const cleared = yield* record(11, configuration);
      yield* insert(cleared, 10);
      yield* store.applyFoundation(cleared);
      const before = JSON.stringify(yield* sql`SELECT * FROM work_task_view_fence`);
      for (const event of applied)
        assert.isFalse((yield* store.applyManagerAllowance(event)).applied);
      assert.strictEqual(JSON.stringify(yield* sql`SELECT * FROM work_task_view_fence`), before);
      const restarted = yield* Effect.gen(function* () {
        const fresh = yield* TaskStore.TaskStore;
        return {
          custody: yield* fresh.readManagerCustody("task"),
          allowance: yield* fresh.readAllowance("task"),
        };
      }).pipe(Effect.provide(Layer.fresh(TaskStore.layer)));
      assert.strictEqual(restarted.custody?.current?.managerThreadId, "manager-2");
      assert.deepStrictEqual(restarted.allowance?.allowance, {
        type: "configured",
        cap: 2,
        consumed: 2,
      });
      assert.strictEqual((yield* sql`SELECT * FROM work_task_accepted_run_debits`).length, 0);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      const rebuilt = yield* Effect.gen(function* () {
        yield* runMigrations();
        const fresh = yield* TaskStore.TaskStore;
        const freshSql = yield* SqlClient.SqlClient;
        assert.strictEqual((yield* freshSql`SELECT * FROM work_task_projections`).length, 0);
        assert.isUndefined(yield* fresh.readFoundation("task"));
        const history = [created, observed, ...applied, cleared];
        for (const [version, value] of history.entries()) yield* insert(value, version);
        const wake = yield* record(12, {
          type: "task.wake-source-recorded",
          commandId: "command-12",
          source: { kind: "result", sourceId: "source", resultRef: null },
          reservationId: null,
          dirtyGeneration: 1,
        });
        yield* insert(wake, 11);
        const replay = Effect.gen(function* () {
          for (const value of history) {
            if (applied.includes(value)) yield* fresh.applyManagerAllowance(value);
            else yield* fresh.applyFoundation(value);
          }
        });
        const error = yield* freshSql
          .withTransaction(replay.pipe(Effect.andThen(fresh.applyManagerAllowance(wake))))
          .pipe(Effect.flip);
        assert.strictEqual("code" in error ? error.code : undefined, "unsupported");
        assert.isUndefined(yield* fresh.readFoundation("task"));
        assert.strictEqual(
          (yield* freshSql<{ generation: number }>`SELECT generation FROM work_task_view_fence`)[0]
            ?.generation,
          0,
        );
        yield* freshSql.withTransaction(replay);
        return {
          custody: yield* fresh.readManagerCustody("task"),
          allowance: yield* fresh.readAllowance("task"),
        };
      }).pipe(
        Effect.provide(
          Layer.fresh(TaskStore.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
          ),
        ),
      );
      assert.deepStrictEqual(rebuilt, restarted);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "rejects custody and allowance conflicts without partial checkpoint or counter writes",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* TaskStore.TaskStore;
      const sql = yield* SqlClient.SqlClient;
      const created = yield* record(1);
      yield* insert(created, 0);
      yield* store.applyFoundation(created);
      const bound = yield* record(2, {
        type: "task.manager-bound",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      yield* insert(bound, 1);
      yield* store.applyManagerAllowance(bound);
      const configured = yield* record(3, {
        type: "task.allowance-configured",
        commandId: "command-3",
        previous: { type: "unconfigured", consumed: 0 },
        resulting: { type: "configured", cap: 1, consumed: 0 },
      });
      yield* insert(configured, 2);
      yield* store.applyManagerAllowance(configured);
      const debit = yield* record(4, {
        type: "task.allowance-debited",
        commandId: "command-4",
        previous: { type: "configured", cap: 1, consumed: 0 },
        resulting: { type: "configured", cap: 1, consumed: 1 },
        allowanceUnit: 1,
      });
      yield* insert(debit, 3);
      yield* store.applyManagerAllowance(debit);
      const snapshot = Effect.gen(function* () {
        const tables = [
          "work_task_projections",
          "work_task_current_state",
          "work_task_manager_bindings",
          "work_task_allowances",
          "work_task_manager_reference_checkpoints",
          "work_task_view_fence",
          "orchestration_events",
        ];
        const values = [];
        for (const table of tables) values.push(yield* sql.unsafe(`SELECT * FROM ${table}`));
        return JSON.stringify(values);
      });
      const reject = Effect.fn("TaskStoreTest.rejectCustody")(function* (
        event: Record<string, unknown>,
        code: string,
        sequence = 5,
        commandId = "command-5",
      ) {
        const before = yield* snapshot;
        const value = yield* record(sequence, event, commandId);
        const error = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* insert(value, sequence - 1);
              return yield* store.applyManagerAllowance(value);
            }),
          )
          .pipe(Effect.flip);
        assert.strictEqual("code" in error ? error.code : undefined, code);
        assert.strictEqual(yield* snapshot, before);
      });
      yield* reject(
        { type: "task.manager-bound", managerThreadId: "other", managerGeneration: 2 },
        "manager",
      );
      yield* reject(
        { type: "task.manager-retired", managerThreadId: "other", managerGeneration: 1 },
        "manager",
      );
      yield* reject(
        {
          type: "task.allowance-configured",
          commandId: "command-5",
          previous: { type: "unconfigured", consumed: 0 },
          resulting: { type: "configured", cap: 3, consumed: 0 },
        },
        "allowance",
      );
      const topup = {
        type: "task.allowance-topped-up",
        attribution: { type: "user", subject: "owner", sessionId: "human" },
        commandId: "command-5",
        previous: { type: "configured", cap: 1, consumed: 1 },
        resulting: { type: "configured", cap: 2, consumed: 1 },
      };
      yield* reject({ ...topup, commandId: "command-3" }, "command", 5, "command-3");
      yield* sql`CREATE TEMP TRIGGER ignore_allowance BEFORE UPDATE ON work_task_allowances BEGIN SELECT RAISE(IGNORE); END`;
      yield* reject(topup, "cas");
      yield* sql`DROP TRIGGER ignore_allowance`;
      yield* sql`CREATE TEMP TRIGGER ignore_custody_fence BEFORE UPDATE ON work_task_view_fence BEGIN SELECT RAISE(IGNORE); END`;
      yield* reject(topup, "cas");
      yield* sql`DROP TRIGGER ignore_custody_fence`;
      yield* sql`CREATE TEMP TRIGGER ignore_custody_marker BEFORE UPDATE ON work_task_manager_reference_checkpoints BEGIN SELECT RAISE(IGNORE); END`;
      yield* reject(topup, "cas");
      yield* sql`DROP TRIGGER ignore_custody_marker`;
      const beforeCorruption = yield* snapshot;
      const rollback = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE work_task_allowances SET consumed=0`;
            assert.strictEqual(
              (yield* store.applyManagerAllowance(debit).pipe(Effect.flip)).code,
              "legacy",
            );
            assert.strictEqual(
              (yield* store.readAllowance("task").pipe(Effect.flip)).code,
              "legacy",
            );
            return yield* Effect.fail("rollback");
          }),
        )
        .pipe(Effect.flip);
      assert.strictEqual(rollback, "rollback");
      assert.strictEqual(yield* snapshot, beforeCorruption);
      const retired = yield* record(5, {
        type: "task.manager-retired",
        managerThreadId: "manager",
        managerGeneration: 1,
      });
      yield* insert(retired, 4);
      yield* store.applyManagerAllowance(retired);
      yield* reject(
        { type: "task.manager-bound", managerThreadId: "manager", managerGeneration: 2 },
        "manager",
        6,
        "command-6",
      );
      yield* reject(
        { type: "task.manager-bound", managerThreadId: "manager", managerGeneration: 1 },
        "manager",
        6,
        "command-6",
      );
      yield* reject(
        { type: "task.manager-retired", managerThreadId: "manager", managerGeneration: 1 },
        "manager",
        6,
        "command-6",
      );
      const next = yield* record(6, { ...topup, commandId: "command-6" });
      yield* insert(next, 5);
      assert.isTrue((yield* store.applyManagerAllowance(next)).applied);
      assert.deepStrictEqual((yield* store.readAllowance("task"))?.allowance, {
        type: "configured",
        cap: 2,
        consumed: 1,
      });
      assert.isUndefined((yield* store.readManagerCustody("task"))?.current);
      assert.isFalse((yield* store.applyManagerAllowance(bound)).applied);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("keeps shared-thread custody task-local and refuses unmaterialized native history", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const store = yield* TaskStore.TaskStore;
    const sql = yield* SqlClient.SqlClient;
    const threadId = "\0manager\ud800";
    for (const [index, taskId] of ["\0task\ud800", "second"].entries()) {
      const offset = index * 10;
      const creation = yield* record(offset + 1, { taskId });
      yield* insert(creation, 0);
      yield* store.applyFoundation(creation);
      const bound = yield* record(
        offset + 2,
        {
          taskId,
          type: "task.manager-bound",
          managerThreadId: threadId,
          managerGeneration: index + 1,
        },
        "shared-command",
      );
      yield* insert(bound, 1);
      yield* store.applyManagerAllowance(bound);
      const configured = yield* record(
        offset + 3,
        {
          taskId,
          type: "task.allowance-configured",
          commandId: "shared-command",
          previous: { type: "unconfigured", consumed: 0 },
          resulting: { type: "configured", cap: index + 1, consumed: 0 },
        },
        "shared-command",
      );
      yield* insert(configured, 2);
      yield* store.applyManagerAllowance(configured);
      const observation = yield* record(offset + 4, {
        ...execution(index + 1),
        taskId,
        manager: { managerThreadId: threadId, managerGeneration: index + 1 },
      });
      yield* insert(observation, 3);
      yield* store.applyFoundation(observation);
      assert.strictEqual(
        (yield* store.readManagerCustody(taskId))?.current?.managerThreadId,
        threadId,
      );
      assert.strictEqual(
        (yield* store.readManagerBinding({ taskId, generation: index + 1 }))?.taskId,
        taskId,
      );
      assert.deepStrictEqual((yield* store.readAllowance(taskId))?.allowance, {
        type: "configured",
        cap: index + 1,
        consumed: 0,
      });
      assert.isUndefined(yield* store.readManagerBinding({ taskId, generation: 99 }));
    }
    for (const [index, type] of ["task.manager-bound", "task.allowance-configured"].entries()) {
      const taskId = `unmaterialized-${index}`;
      const sequence = 30 + index * 10;
      const creation = yield* record(sequence, { taskId });
      yield* insert(creation, 0);
      yield* store.applyFoundation(creation);
      const value = yield* record(
        sequence + 1,
        type === "task.manager-bound"
          ? { taskId, type, managerThreadId: "old", managerGeneration: 1 }
          : {
              taskId,
              type,
              commandId: `command-${sequence + 1}`,
              previous: { type: "unconfigured", consumed: 0 },
              resulting: { type: "configured", cap: 1, consumed: 0 },
            },
      );
      yield* insert(value, 1);
      yield* sql`UPDATE work_task_current_state SET last_applied_sequence=${sequence + 1} WHERE task_id=${taskId}`;
      yield* store.auditManagerReferences({ taskId, expectedCheckpointSequence: sequence + 1 });
      assert.strictEqual(
        (yield* store.applyManagerAllowance(value).pipe(Effect.flip)).code,
        "legacy",
      );
      assert.isFalse((yield* store.applyFoundation(creation)).applied);
    }
    const unsafe = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE work_task_allowances SET configured_cap=9007199254740992 WHERE task_id='second'`;
          assert.strictEqual(
            (yield* store.readAllowance("second").pipe(Effect.flip)).code,
            "invalid",
          );
          return yield* Effect.fail("rollback");
        }),
      )
      .pipe(Effect.flip);
    assert.strictEqual(unsafe, "rollback");
    const plans = [
      [
        "SELECT * FROM work_task_manager_bindings WHERE task_id=? AND retired_at IS NULL ORDER BY generation DESC LIMIT 1",
        ["second"],
        "work_task_manager_current_idx",
      ],
      [
        "SELECT * FROM work_task_manager_bindings WHERE task_id=? ORDER BY generation DESC LIMIT 1",
        ["second"],
        "PRIMARY KEY",
      ],
      [
        "SELECT * FROM work_task_manager_bindings WHERE task_id=? AND thread_id=?",
        ["second", "manager"],
        "INDEX",
      ],
      [
        "SELECT * FROM work_task_allowances WHERE task_id=?",
        ["second"],
        "sqlite_autoindex_work_task_allowances",
      ],
      [
        "SELECT * FROM orchestration_events INDEXED BY idx_orch_events_command_id WHERE aggregate_kind='task' AND application_event_version=2 AND stream_id=json_extract(?,'$') AND command_id=json_extract(?,'$') AND event_type IN ('task.allowance-configured','task.allowance-topped-up','task.allowance-debited') AND sequence<=? ORDER BY sequence DESC LIMIT 1",
        ['"second"', '"shared-command"', 14],
        "idx_orch_events_command_id",
      ],
    ] as const;
    for (const [query, parameters, index] of plans) {
      const rows = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);
      const plan = rows.map((row) => row.detail).join(" ");
      assert.include(plan, "SEARCH");
      assert.include(plan, index);
      assert.notInclude(plan, "TEMP B-TREE");
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("reads real native records and rejects published pruning before physical deletion", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const created = yield* record(1);
    yield* insert(created, 0);
    assert.deepStrictEqual(
      (yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" })).recorded,
      created,
    );
    const first = yield* store.readFoundationSource({ taskId: "task", checkpointSequence: null });
    assert.deepStrictEqual(first.next?.recorded, created);
    const end = yield* store.readFoundationSource({ taskId: "task", checkpointSequence: 1 });
    assert.isUndefined(end.next);
    yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,1,${timestamp})`;
    const rejected = yield* store
      .readNativeSequence({ taskId: "task", sequence: 1 })
      .pipe(Effect.flip);
    assert.strictEqual(rejected.code, "source_unavailable");
    assert.strictEqual((yield* sql`SELECT COUNT(*) AS n FROM orchestration_events`)[0]!.n, 1);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "preserves canonical native identity bytes and rejects replacement decoding and aliases",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      for (const [i, identity] of ["\u0000x", "\ud800", "\udc00", "😀", "plain"].entries()) {
        const value = yield* record(
          i + 1,
          { taskId: identity, id: `event:${identity}` },
          `command:${identity}`,
        );
        yield* insert(value, 0);
        assert.deepStrictEqual(
          (yield* store.readNativeEvent({ taskId: identity, eventId: value.event.id })).recorded,
          value,
        );
        assert.deepStrictEqual(
          (yield* store.readNativeSequence({ taskId: identity, sequence: i + 1 })).recorded,
          value,
        );
      }
      const created = yield* record(20);
      const changed = yield* record(21, configuration, null);
      yield* insert(created, 0);
      yield* insert(changed, 1);
      assert.isNull(
        (yield* store.readNativeSequence({ taskId: "task", sequence: 21 })).recorded.context
          .commandId,
      );
      for (const hex of [
        "80",
        "C080",
        "E08080",
        "F0808080",
        "F4908080",
        "E282",
        "F5808080",
        "EDA0BDEDB880",
      ]) {
        yield* sql`UPDATE orchestration_events SET event_id=CAST(unhex(${hex}) AS TEXT) WHERE sequence=21`;
        const error = yield* store
          .readNativeSequence({ taskId: "task", sequence: 21 })
          .pipe(Effect.flip);
        assert.strictEqual(error.code, "source_bytes");
      }
      yield* sql`UPDATE orchestration_events SET event_id='event-21',command_id='' WHERE sequence=21`;
      assert.strictEqual(
        (yield* store.readNativeSequence({ taskId: "task", sequence: 21 }).pipe(Effect.flip)).code,
        "source_invalid",
      );
      yield* sql`UPDATE orchestration_events SET command_id=NULL,payload_json=json_set(payload_json,'$.unknown','discarded') WHERE sequence=21`;
      assert.deepStrictEqual(
        (yield* store.readNativeSequence({ taskId: "task", sequence: 21 })).recorded,
        changed,
      );
      for (const [column, hex] of [
        ["command_id", "EDA0BDEDB880"],
        [
          "payload_json",
          Buffer.from(JSON.stringify({ ...changed.event, unknown: "😀" }))
            .toString("hex")
            .toUpperCase()
            .replace("F09F9880", "EDA0BDEDB880"),
        ],
      ]) {
        yield* sql.unsafe(
          `UPDATE orchestration_events SET ${column}=CAST(unhex(?) AS TEXT) WHERE sequence=21`,
          [hex!],
        );
        assert.strictEqual(
          (yield* store.readNativeSequence({ taskId: "task", sequence: 21 }).pipe(Effect.flip))
            .code,
          "source_bytes",
        );
        yield* sql`UPDATE orchestration_events SET command_id=NULL,payload_json=${JSON.stringify(changed.event)} WHERE sequence=21`;
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect("fails closed on native header, counter, type and context contradictions", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const created = yield* record(1);
    const changed = yield* record(2, configuration);
    yield* insert(created, 0);
    yield* insert(changed, 1);
    for (const mutation of [
      "event_id=' padded '",
      "stream_id='another-task'",
      "event_type='task.unknown'",
      "occurred_at='2026-11-17T00:00:00.000Z'",
      "command_id=' padded '",
      "aggregate_kind='thread'",
      "application_event_version=1",
      "payload_json='{'",
      "stream_version=9007199254740992",
      "stream_version=-1",
      "stream_version=1.5",
      "stream_version='bad'",
      "payload_json=json_set(payload_json,'$.id','different')",
      "payload_json=json_set(payload_json,'$.type','task.unknown')",
    ]) {
      yield* sql.unsafe(`UPDATE orchestration_events SET ${mutation} WHERE sequence=2`);
      assert.instanceOf(
        yield* store.readNativeSequence({ taskId: "task", sequence: 2 }).pipe(Effect.flip),
        TaskStore.TaskSourceError,
      );
      yield* sql`DELETE FROM orchestration_events WHERE sequence=2`;
      yield* insert(changed, 1);
    }
    assert.strictEqual(
      (yield* store
        .readNativeSequence({ taskId: "task", sequence: 2, eventId: "other" })
        .pipe(Effect.flip)).code,
      "source_identity",
    );
    assert.strictEqual(
      (yield* store.readNativeEvent({ taskId: "task", eventId: "absent" }).pipe(Effect.flip)).code,
      "source_unavailable",
    );
    yield* sql`UPDATE orchestration_events SET sequence=9007199254740992 WHERE sequence=2`.pipe(
      Effect.provideService(SqlClient.SafeIntegers, true),
    );
    assert.strictEqual(
      (yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" }).pipe(Effect.flip)).code,
      "source_counter",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "reads covered foundation witnesses without erasing manager history or inventing a next record",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const events = [
        yield* record(1),
        yield* record(
          2,
          {
            type: "task.definition-accepted",
            attribution: { type: "user", subject: "human", sessionId: "session" },
          },
          "accept",
        ),
        yield* record(3, {
          type: "task.definition-updated",
          definition: { ...definition, revision: 2 },
          definitionRevision: 2,
        }),
        yield* record(4, configuration),
        yield* record(5, execution(1)),
        yield* record(6, execution(2)),
        yield* record(7, execution(null)),
      ];
      for (const [version, value] of events.entries()) yield* insert(value, version);
      const result = yield* store.readFoundationSource({
        taskId: "task",
        checkpointSequence: 7,
        managerGeneration: 1,
        acceptanceCommandId: "accept",
        incomingEventId: "event-2",
      });
      assert.isUndefined(result.next);
      assert.strictEqual(result.incoming?.recorded.event.id, "event-2");
      assert.strictEqual(result.latestDefinition?.recorded.event.id, "event-3");
      assert.strictEqual(result.configuration?.recorded.event.id, "event-4");
      assert.strictEqual(result.manager?.recorded.event.id, "event-5");
      assert.strictEqual(result.highestManager?.recorded.event.id, "event-6");
      assert.strictEqual(result.acceptance?.recorded.event.id, "event-2");
      const before = yield* store.readFoundationSource({ taskId: "task", checkpointSequence: 1 });
      assert.strictEqual(before.next?.recorded.event.id, "event-2");
      assert.isUndefined(before.highestManager);
      assert.strictEqual(
        (yield* store
          .readFoundationSource({
            taskId: "task",
            checkpointSequence: 1,
            incomingEventId: "event-4",
          })
          .pipe(Effect.flip)).code,
        "source_version",
      );
      yield* sql`DELETE FROM orchestration_events WHERE sequence=2`;
      assert.strictEqual(
        (yield* store
          .readFoundationSource({ taskId: "task", checkpointSequence: 1 })
          .pipe(Effect.flip)).code,
        "source_version",
      );
      assert.strictEqual(
        (yield* store
          .readFoundationSource({ taskId: "task", checkpointSequence: 2 })
          .pipe(Effect.flip)).code,
        "source_unavailable",
      );
      yield* sql`DELETE FROM orchestration_events WHERE sequence=1`;
      assert.strictEqual(
        (yield* store
          .readFoundationSource({ taskId: "task", checkpointSequence: null })
          .pipe(Effect.flip)).code,
        "source_unavailable",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "keeps history pages on an actual fixed fence with exact anchors and no skipped holes",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      for (let i = 1; i <= 6; i++)
        yield* insert(yield* record(i, i === 1 ? {} : configuration), i - 1);
      const first = yield* store.readNativeHistory({
        taskId: "task",
        throughSequence: 5,
        limit: 2,
      });
      assert.deepStrictEqual(
        first.records.map((r) => r.recorded.context.sequence),
        [1, 2],
      );
      assert.isDefined(first.next);
      const second = yield* store.readNativeHistory({
        taskId: "task",
        throughSequence: 5,
        limit: 2,
        after: first.next!,
      });
      assert.deepStrictEqual(
        second.records.map((r) => r.recorded.context.sequence),
        [3, 4],
      );
      const third = yield* store.readNativeHistory({
        taskId: "task",
        throughSequence: 5,
        limit: 2,
        after: second.next!,
      });
      assert.deepStrictEqual(
        third.records.map((r) => r.recorded.context.sequence),
        [5],
      );
      assert.isUndefined(third.next);
      for (const input of [
        { throughSequence: 7 },
        { throughSequence: 6, after: first.next! },
        { throughSequence: 4, after: first.next! },
        { throughSequence: 1, after: first.next! },
        { throughSequence: 5, after: { ...first.next!, streamVersion: 4 } },
        { throughSequence: 5, after: { ...first.next!, eventId: "wrong" } },
        { throughSequence: 5, after: { ...first.next!, priorPrunedThrough: 1 } },
        { throughSequence: 5, limit: 101 },
        { throughSequence: 5, limit: 0 },
      ])
        assert.instanceOf(
          yield* store.readNativeHistory({ taskId: "task", ...input }).pipe(Effect.flip),
          TaskStore.TaskSourceError,
        );
      for (const field of ["throughSequence", "priorPrunedThrough"]) {
        const missing: NonNullable<typeof first.next> = { ...first.next! };
        Reflect.deleteProperty(missing, field);
        assert.instanceOf(
          yield* store
            .readNativeHistory({ taskId: "task", throughSequence: 5, after: missing })
            .pipe(Effect.flip),
          TaskStore.TaskSourceError,
        );
      }
      yield* insert(yield* record(7, configuration), 6);
      assert.strictEqual(
        (yield* store
          .readNativeHistory({ taskId: "task", throughSequence: 7, after: first.next! })
          .pipe(Effect.flip)).code,
        "source_identity",
      );
      yield* sql`DELETE FROM orchestration_events WHERE sequence=3`;
      assert.strictEqual(
        (yield* store
          .readNativeHistory({ taskId: "task", throughSequence: 5, after: first.next! })
          .pipe(Effect.flip)).code,
        "source_version",
      );
      yield* insert(yield* record(3, configuration), 2);
      yield* sql`UPDATE orchestration_events SET event_type='task.unknown' WHERE sequence=3`;
      assert.instanceOf(
        yield* store
          .readNativeHistory({ taskId: "task", throughSequence: 5, after: first.next! })
          .pipe(Effect.flip),
        TaskStore.TaskSourceError,
      );
      yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,2,${timestamp})`;
      assert.strictEqual(
        (yield* store
          .readNativeHistory({ taskId: "task", throughSequence: 5, after: first.next! })
          .pipe(Effect.flip)).code,
        "source_unavailable",
      );
      for (let i = 20; i <= 23; i++)
        yield* insert(
          yield* record(i, { ...(i === 20 ? {} : configuration), taskId: "late" }),
          i - 20,
        );
      yield* sql`UPDATE orchestration_v2_projection_metadata SET last_sequence=10 WHERE projection_name='event-log-pruned-through'`;
      const retained = yield* store.readNativeHistory({
        taskId: "late",
        throughSequence: 23,
        limit: 1,
      });
      assert.strictEqual(retained.next?.priorPrunedThrough, 10);
      yield* sql`DELETE FROM orchestration_v2_projection_metadata WHERE projection_name='event-log-pruned-through'`;
      assert.strictEqual(
        (yield* store
          .readNativeHistory({ taskId: "late", throughSequence: 23, after: retained.next! })
          .pipe(Effect.flip)).code,
        "source_unavailable",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("honors initial metadata without changing native source", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    yield* insert(yield* record(1), 0);
    yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-prune-mark',2,1,${timestamp})`;
    const before = yield* sql`SELECT * FROM orchestration_events`;
    yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" });
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_events`, before);
    for (const mutation of [
      "last_sequence=-1",
      "last_sequence=1.5",
      "last_sequence='bad'",
      "last_sequence=9007199254740992",
      "schema_version=999",
      "updated_at='bad'",
    ]) {
      yield* sql.unsafe(
        `UPDATE orchestration_v2_projection_metadata SET ${mutation} WHERE projection_name='event-log-prune-mark'`,
      );
      assert.instanceOf(
        yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" }).pipe(Effect.flip),
        TaskStore.TaskSourceError,
      );
      yield* sql`UPDATE orchestration_v2_projection_metadata SET last_sequence=1,schema_version=2,updated_at=${timestamp} WHERE projection_name='event-log-prune-mark'`;
    }
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "uses native lookup indexes and retains old-prefix manager witnesses beneath a future tail",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      yield* insert(yield* record(1), 0);
      for (let i = 2; i <= 102; i++) yield* insert(yield* record(i, execution(i - 1)), i - 1);
      const page = yield* store.readNativeHistory({
        taskId: "task",
        throughSequence: 102,
        limit: 100,
      });
      assert.strictEqual(page.records.length, 100);
      assert.strictEqual(
        (yield* store.readNativeHistory({
          taskId: "task",
          throughSequence: 102,
          after: page.next!,
        })).records.length,
        2,
      );
      assert.strictEqual(
        (yield* store.readFoundationSource({
          taskId: "task",
          checkpointSequence: 2,
          managerGeneration: 1,
        })).highestManager?.recorded.event.id,
        "event-2",
      );
      const expression =
        "CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.manager.managerGeneration') END";
      const predicate =
        "aggregate_kind='task' AND application_event_version=2 AND event_type='task.execution-updated' AND CASE WHEN json_valid(payload_json) THEN json_type(payload_json,'$.manager') END='object'";
      for (const [where, index] of [
        ["event_id='event-2'", "sqlite_autoindex_orchestration_events"],
        [
          "command_id='accept' AND aggregate_kind='task' AND application_event_version=2 AND stream_id='task' AND event_type='task.definition-accepted' AND sequence<=2 ORDER BY sequence LIMIT 1",
          "idx_orch_events_command_id",
        ],
        ["sequence=2", "INTEGER PRIMARY KEY"],
        [
          "aggregate_kind='task' AND stream_id='task' AND stream_version=1",
          "idx_orch_events_stream_version",
        ],
        [
          "aggregate_kind='task' AND stream_id='task' AND sequence<=2 ORDER BY sequence DESC LIMIT 1",
          "idx_orch_events_stream_sequence",
        ],
        [
          "aggregate_kind='task' AND stream_id='task' AND stream_version>1 AND stream_version<=100 ORDER BY stream_version LIMIT 51",
          "idx_orch_events_stream_version",
        ],
        [
          "aggregate_kind='task' AND application_event_version=2 AND stream_id='task' AND event_type='task.configuration-updated' AND sequence<=2 ORDER BY sequence DESC LIMIT 1",
          "idx_task_source_type_sequence",
        ],
        [
          `${predicate} AND stream_id='task' AND ${expression}=1 AND sequence<=2 ORDER BY sequence DESC LIMIT 1`,
          "idx_task_source_manager_sequence",
        ],
        [
          `${predicate} AND stream_id='task' AND sequence<=2 ORDER BY ${expression} DESC LIMIT 1`,
          "idx_task_source_manager_sequence",
        ],
      ]) {
        const hint =
          index!.startsWith("idx_task_source_") || index === "idx_orch_events_command_id"
            ? ` INDEXED BY ${index}`
            : "";
        const plan = JSON.stringify(
          yield* sql.unsafe(
            `EXPLAIN QUERY PLAN SELECT event_id FROM orchestration_events${hint} WHERE ${where}`,
          ),
        );
        assert.include(plan, index!);
        assert.include(plan, "SEARCH");
        assert.notInclude(plan, "TEMP B-TREE");
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "keeps a nested source read on its outer SQLite snapshot while another connection publishes pruning",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "task-source-" });
        const filename = path.join(directory, "source.sqlite");
        const program = Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`PRAGMA journal_mode=WAL`;
            yield* runMigrations();
            yield* insert(yield* record(1), 0);
            const store = yield* TaskStore.TaskStore;
            const writer = yield* Effect.acquireRelease(
              Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
              (db) => Effect.sync(() => db.close()),
            );
            yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" });
                  yield* Effect.sync(() =>
                    writer.exec(
                      `INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,1,'${timestamp}')`,
                    ),
                  );
                  assert.strictEqual(
                    (yield* store.readNativeSequence({ taskId: "task", sequence: 1 })).recorded
                      .event.id,
                    "event-1",
                  );
                }),
              )
              .pipe(NodeSqliteClient.readOnly);
            assert.strictEqual(
              (yield* store
                .readNativeEvent({ taskId: "task", eventId: "event-1" })
                .pipe(Effect.flip)).code,
              "source_unavailable",
            );
            yield* Effect.sync(() =>
              writer.exec(
                "DELETE FROM orchestration_v2_projection_metadata WHERE projection_name='event-log-pruned-through'",
              ),
            );
            const rollback = yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" });
                  yield* sql`UPDATE work_task_view_fence SET generation=1`;
                  yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,1,${timestamp})`;
                  assert.strictEqual(
                    (yield* store
                      .readNativeEvent({ taskId: "task", eventId: "event-1" })
                      .pipe(Effect.flip)).code,
                    "source_unavailable",
                  );
                  return yield* Effect.fail("rollback fixture");
                }),
              )
              .pipe(Effect.flip);
            assert.strictEqual(rollback, "rollback fixture");
            assert.strictEqual(
              (yield* sql`SELECT generation FROM work_task_view_fence`)[0]!.generation,
              0,
            );
            yield* store.readNativeEvent({ taskId: "task", eventId: "event-1" });
          }),
        );
        yield* program.pipe(
          Effect.provide(
            TaskStore.layer.pipe(
              Layer.provideMerge(NodeSqliteClient.layer({ filename, readerWorker: true })),
            ),
          ),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "projects foundation facts atomically and replays old source without changing the view",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const events = [
        yield* record(1),
        yield* record(2, {
          type: "task.definition-accepted",
          attribution: { type: "user", subject: "human", sessionId: "session" },
        }),
        yield* record(3, configuration),
        yield* record(4, execution(1)),
        yield* record(5, { type: "task.thread-linked", threadId: "ordinary" }, "command-2"),
        yield* record(6, { type: "task.thread-unlinked", threadId: "ordinary" }),
        yield* record(7, {
          type: "task.definition-updated",
          definitionRevision: 2,
          definition: { ...definition, revision: 2, title: "Draft" },
        }),
      ];
      for (const [version, value] of events.entries()) {
        yield* insert(value, version);
        assert.isTrue((yield* store.applyFoundation(value)).applied);
      }
      const current = yield* store.readFoundation("task");
      assert.strictEqual(current?.projection.definition.title, "Draft");
      assert.strictEqual(current?.accepted?.definition.title, "Outcome");
      assert.strictEqual(current?.draft?.title, "Draft");
      assert.strictEqual(current?.projection.current?.desiredMode, "active");
      assert.isNull(current?.projection.current?.executionMode);
      assert.isNull(current?.projection.current?.attention);
      assert.isNull(current?.projection.current?.completion);
      assert.isNull(current?.projection.current?.archived);
      assert.isTrue(current?.sourceVerified);
      const link = yield* store.readThreadLink({ taskId: "task", linkEventId: "event-5" });
      assert.strictEqual(link?.unlinked?.eventId, "event-6");
      const before = yield* sql`SELECT * FROM work_task_view_fence`;
      assert.isFalse((yield* store.applyFoundation(events[0]!)).applied);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_view_fence`, before);
      assert.deepStrictEqual(yield* store.readFoundation("task"), current);
    }).pipe(Effect.provide(testLayer)),
);

const humanAcceptance = {
  type: "task.definition-accepted",
  attribution: { type: "user", subject: "human", sessionId: "session" },
};
const projectionSnapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return [
    yield* sql`SELECT * FROM work_task_projections`,
    yield* sql`SELECT * FROM work_task_current_state`,
    yield* sql`SELECT * FROM work_task_accepted_definitions`,
    yield* sql`SELECT * FROM work_task_thread_links`,
    yield* sql`SELECT * FROM work_task_view_fence`,
  ];
});

it.effect("rejects contradictory foundation identities without partial writes", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const draft = { ...definition, revision: 2, title: "Draft two" };
    const seed = [
      yield* record(1),
      yield* record(2, humanAcceptance),
      yield* record(3, configuration),
      yield* record(4, execution(1)),
      yield* record(5, {
        type: "task.definition-updated",
        definitionRevision: 2,
        definition: draft,
      }),
      yield* record(6, { ...execution(2), definitionRevision: 2 }),
      yield* record(7, { ...execution(null), definitionRevision: 2 }),
    ];
    for (const [i, value] of seed.entries()) {
      yield* insert(value, i);
      yield* store.applyFoundation(value);
    }
    const before = yield* projectionSnapshot;
    const changedPolicy = {
      version: 1,
      rules: [{ id: "r", action: "edit", target: "file", decision: "allow" }],
    };
    for (const [patch, command, code] of [
      [{ type: "task.definition-updated", definitionRevision: 1, definition }, "c", "revision"],
      [{ ...humanAcceptance, definition: { ...draft, title: "Different" } }, "c", "definition"],
      [
        {
          ...humanAcceptance,
          definitionRevision: 3,
          definition: { ...definition, revision: 3, actionPolicy: changedPolicy },
        },
        "c",
        "policy",
      ],
      [
        { ...humanAcceptance, definitionRevision: 3, definition: { ...definition, revision: 3 } },
        "command-2",
        "definition",
      ],
      [
        { ...configuration, completionPolicy: { type: "configured", policy: { type: "manual" } } },
        "c",
        "policy",
      ],
      [
        { ...execution(2), manager: { managerThreadId: "other", managerGeneration: 2 } },
        "c",
        "manager",
      ],
      [execution(1), "c", "manager"],
      [{ type: "task.thread-unlinked", threadId: "missing" }, "c", "link"],
    ] as const) {
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const value = yield* record(8, { definitionRevision: 2, ...patch }, command);
            yield* insert(value, 7);
            assert.strictEqual((yield* store.applyFoundation(value).pipe(Effect.flip)).code, code);
            assert.deepStrictEqual(yield* projectionSnapshot, before);
            return yield* Effect.fail("fixture rollback");
          }),
        )
        .pipe(Effect.flip);
    }
    const accepted = yield* record(8, {
      ...humanAcceptance,
      definitionRevision: 3,
      definition: { ...definition, revision: 3 },
    });
    yield* insert(accepted, 7);
    yield* store.applyFoundation(accepted);
    assert.strictEqual((yield* store.readFoundation("task"))?.draft?.revision, 2);
    assert.strictEqual((yield* store.readFoundation("task"))?.accepted?.definitionRevision, 3);
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const duplicate = yield* record(9, {
            ...humanAcceptance,
            definitionRevision: 3,
            definition: { ...definition, revision: 3 },
          });
          yield* insert(duplicate, 8);
          assert.strictEqual(
            (yield* store.applyFoundation(duplicate).pipe(Effect.flip)).code,
            "definition",
          );
          return yield* Effect.fail("fixture rollback");
        }),
      )
      .pipe(Effect.flip);
    const configSource = yield* record(9, {
      ...configuration,
      definitionRevision: 3,
      completionPolicyRevision: 2,
    });
    const oldTime = "2000-01-01T00:00:00.000Z";
    const configured = yield* decode({
      event: { ...configSource.event, occurredAt: oldTime },
      context: { ...configSource.context, occurredAt: oldTime },
    });
    yield* insert(configured, 8);
    yield* store.applyFoundation(configured);
    assert.strictEqual((yield* store.readFoundation("task"))?.projection.updatedAt, oldTime);
    const linked = yield* record(10, {
      type: "task.thread-linked",
      threadId: "ordinary",
      definitionRevision: 3,
      completionPolicyRevision: 3,
    });
    yield* insert(linked, 9);
    yield* store.applyFoundation(linked);
    const nextPolicy = yield* record(11, {
      ...configuration,
      definitionRevision: 3,
      completionPolicyRevision: 3,
      completionPolicy: { type: "configured", policy: { type: "manual" } },
    });
    yield* insert(nextPolicy, 10);
    yield* store.applyFoundation(nextPolicy);
    const duplicateLink = yield* record(12, {
      type: "task.thread-linked",
      threadId: "ordinary",
      definitionRevision: 3,
      completionPolicyRevision: 3,
    });
    yield* insert(duplicateLink, 11);
    assert.strictEqual(
      (yield* store.applyFoundation(duplicateLink).pipe(Effect.flip)).code,
      "link",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "keeps legacy projections unverified and never skips unsupported or unavailable source",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const created = yield* record(1);
      yield* insert(created, 0);
      yield* sql`INSERT INTO work_task_projections VALUES('task','project',1,1,1,1,${JSON.stringify(definition)},'legacy','legacy')`;
      assert.isFalse((yield* store.readFoundation("task"))?.sourceVerified);
      assert.isNull((yield* store.readFoundation("task"))?.projection.current);
      assert.strictEqual((yield* store.applyFoundation(created).pipe(Effect.flip)).code, "legacy");
      yield* sql`INSERT INTO work_task_current_state(task_id) VALUES('task')`;
      assert.isNull((yield* store.readFoundation("task"))?.projection.current?.lastSequence);
      assert.strictEqual((yield* store.applyFoundation(created).pipe(Effect.flip)).code, "legacy");
      yield* sql`DELETE FROM work_task_current_state`;
      yield* sql`DELETE FROM work_task_projections`;
      yield* store.applyFoundation(created);
      const unsupported = yield* record(2, { type: "task.archived", archived: true });
      const later = yield* record(3, configuration);
      yield* insert(unsupported, 1);
      yield* insert(later, 2);
      const before = yield* projectionSnapshot;
      assert.strictEqual(
        (yield* store.applyFoundation(unsupported).pipe(Effect.flip)).code,
        "unsupported",
      );
      assert.strictEqual(
        (yield* store.applyFoundation(later).pipe(Effect.flip)).code,
        "source_version",
      );
      assert.deepStrictEqual(yield* projectionSnapshot, before);
      yield* sql`UPDATE work_task_current_state SET last_applied_sequence=2,archived=1`;
      assert.strictEqual(
        (yield* store.applyFoundation(later).pipe(Effect.flip)).code,
        "qualification",
      );
      yield* store.auditManagerReferences({ taskId: "task", expectedCheckpointSequence: 2 });
      yield* store.applyFoundation(later);
      assert.strictEqual((yield* store.readFoundation("task"))?.projection.current?.archived, 1);
      yield* sql`INSERT INTO orchestration_v2_projection_metadata VALUES('event-log-pruned-through',2,1,${timestamp})`;
      assert.strictEqual(
        (yield* store.applyFoundation(created).pipe(Effect.flip)).code,
        "source_unavailable",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("uses durable CAS and a finite fence with rollback and concurrent exact replay", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const created = yield* record(1);
    yield* insert(created, 0);
    yield* store.applyFoundation(created);
    const accepted = yield* record(2, humanAcceptance);
    yield* insert(accepted, 1);
    const before = yield* projectionSnapshot;
    yield* sql`CREATE TEMP TRIGGER stale_revision BEFORE UPDATE OF last_applied_sequence ON work_task_current_state
      BEGIN UPDATE work_task_projections SET lifecycle_revision=lifecycle_revision+1 WHERE task_id=NEW.task_id; END`;
    assert.strictEqual((yield* store.applyFoundation(accepted).pipe(Effect.flip)).code, "cas");
    assert.deepStrictEqual(yield* projectionSnapshot, before);
    yield* sql`DROP TRIGGER stale_revision`;
    yield* sql`CREATE TEMP TRIGGER fail_fence BEFORE UPDATE ON work_task_view_fence BEGIN SELECT RAISE(ABORT,'fixture'); END`;
    assert.strictEqual((yield* store.applyFoundation(accepted).pipe(Effect.flip)).code, "sql");
    assert.deepStrictEqual(yield* projectionSnapshot, before);
    yield* sql`DROP TRIGGER fail_fence`;
    yield* sql`UPDATE work_task_view_fence SET generation=9007199254740991`;
    assert.strictEqual((yield* store.applyFoundation(accepted).pipe(Effect.flip)).code, "fence");
    yield* sql`UPDATE work_task_view_fence SET generation=1,high_water_sequence=9007199254740991`;
    const outcomes = yield* Effect.all(
      [store.applyFoundation(accepted), store.applyFoundation(accepted)],
      { concurrency: 2 },
    );
    assert.strictEqual(outcomes.filter((value) => value.applied).length, 1);
    assert.strictEqual(
      (yield* sql`SELECT high_water_sequence FROM work_task_view_fence`)[0]!.high_water_sequence,
      Number.MAX_SAFE_INTEGER,
    );
    const reloaded = yield* Effect.gen(function* () {
      const fresh = yield* TaskStore.TaskStore;
      return yield* fresh.applyFoundation(created);
    }).pipe(Effect.provide(TaskStore.layer));
    assert.isFalse(reloaded.applied);
    assert.strictEqual(reloaded.lastSequence, 2);
    const altered = yield* decode({
      ...created,
      event: { ...created.event, definition: { ...definition, title: "Altered" } },
    });
    assert.strictEqual((yield* store.applyFoundation(altered).pipe(Effect.flip)).code, "replay");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("roundtrips projection and link identity bytes without owning ordinary threads", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('p','Project','/p','[]','raw','raw')`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
      VALUES('ordinary','p','Thread','codex','full-access','default','raw','raw','{}')`;
    const ordinary = yield* sql`SELECT * FROM orchestration_v2_projection_threads`;
    for (const [i, taskId] of ["\0task", "\ud800", "\udc00", "😀"].entries()) {
      const base = i * 10 + 1;
      const values = [
        yield* record(base, {
          taskId,
          projectId: taskId,
          definition: { ...definition, data: taskId },
        }),
        yield* record(base + 1, {
          ...humanAcceptance,
          taskId,
          id: `accepted:${taskId}`,
          definition: { ...definition, data: taskId },
        }),
        yield* record(base + 2, {
          ...execution(1),
          taskId,
          manager: { managerThreadId: taskId, managerGeneration: 1 },
        }),
        yield* record(base + 3, {
          type: "task.thread-linked",
          taskId,
          threadId: "ordinary",
          id: `link:${taskId}`,
        }),
        yield* record(base + 4, {
          type: "task.thread-unlinked",
          taskId,
          threadId: "ordinary",
          id: `unlink:${taskId}`,
        }),
        yield* record(base + 5, {
          type: "task.thread-linked",
          taskId,
          threadId: "ordinary",
          id: `relink:${taskId}`,
        }),
      ];
      for (const [version, value] of values.entries()) {
        yield* insert(value, version);
        yield* store.applyFoundation(value);
      }
      const read = yield* store.readFoundation(taskId);
      assert.strictEqual(read?.projection.taskId, taskId);
      assert.strictEqual(read?.projection.projectId, taskId);
      assert.strictEqual(read?.projection.current?.managerThreadId, taskId);
      assert.strictEqual(
        (yield* store.readAcceptedDefinition({ taskId, revision: 1 }))?.eventId,
        `accepted:${taskId}`,
      );
      assert.strictEqual(
        (yield* store.readThreadLink({ taskId, linkEventId: `link:${taskId}` }))?.unlinked?.eventId,
        `unlink:${taskId}`,
      );
      assert.isNull(
        (yield* store.readThreadLink({ taskId, linkEventId: `relink:${taskId}` }))?.unlinked,
      );
    }
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_projection_threads`, ordinary);
    assert.strictEqual(
      (yield* sql`SELECT count(*) AS n FROM work_task_thread_links WHERE unlink_event_id IS NULL`)[0]!
        .n,
      4,
    );
    yield* sql`UPDATE work_task_current_state SET manager_thread_id=CAST(unhex('EDA0BDEDB880') AS TEXT) WHERE task_id=char(0)||'task'`;
    assert.strictEqual((yield* store.readFoundation("\0task").pipe(Effect.flip)).code, "invalid");
    yield* sql`UPDATE work_task_current_state SET manager_thread_id=char(0)||'task' WHERE task_id=char(0)||'task'`;
    yield* sql`UPDATE work_task_projections SET definition_json='{"revision":1}' WHERE task_id=char(0)||'task'`;
    assert.strictEqual((yield* store.readFoundation("\0task").pipe(Effect.flip)).code, "invalid");
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "rebuilds an empty target once from native source and matches pure foundation reduction",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* TaskStore.TaskStore;
      const draft = { ...definition, revision: 2, title: "New draft" };
      const events = [
        yield* record(1),
        yield* record(2, humanAcceptance),
        yield* record(3, configuration),
        yield* record(4, execution(1)),
        yield* record(5, { type: "task.thread-linked", threadId: "ordinary" }),
        yield* record(6, { type: "task.thread-unlinked", threadId: "ordinary" }),
        yield* record(7, {
          type: "task.definition-updated",
          definitionRevision: 2,
          definition: draft,
        }),
        yield* record(8, { ...humanAcceptance, definitionRevision: 2, definition: draft }),
        yield* record(9, {
          type: "task.thread-linked",
          threadId: "ordinary",
          definitionRevision: 2,
        }),
        yield* record(10, { ...execution(null), definitionRevision: 2 }),
      ];
      let pure: ReturnType<typeof reduceTaskFoundation> | undefined;
      for (const [version, value] of events.entries()) {
        yield* insert(value, version);
        const event = value.event;
        switch (event.type) {
          case "task.created":
          case "task.definition-accepted":
          case "task.definition-updated":
          case "task.configuration-updated":
          case "task.execution-updated":
          case "task.thread-linked":
          case "task.thread-unlinked":
            pure = reduceTaskFoundation(pure, { event, context: value.context });
            break;
          default:
            assert.fail("Unsupported fixture");
        }
      }
      const journal =
        yield* sql`SELECT sequence,hex(payload_json) AS payload FROM orchestration_events ORDER BY sequence`;
      const rebuilt = yield* store.rebuildFoundation({ taskId: "task", throughSequence: 10 });
      assert.deepStrictEqual(rebuilt?.projection.definition, pure!.definition.event.definition);
      assert.deepStrictEqual(rebuilt?.draft, pure!.draft.event.definition);
      assert.strictEqual(rebuilt?.accepted?.definitionRevision, 2);
      assert.deepStrictEqual(rebuilt?.projection.current?.configuration, {
        allowanceOverride: null,
        completionPolicy: { type: "inherit" },
      });
      assert.isNull(rebuilt?.projection.current?.managerThreadId);
      assert.strictEqual(rebuilt?.projection.current?.lastSequence, pure!.lastSequence);
      for (const [id, link] of pure!.links) {
        const stored = yield* store.readThreadLink({ taskId: "task", linkEventId: id });
        assert.deepStrictEqual(stored?.attribution, link.linked.event.attribution);
        assert.strictEqual(stored?.unlinked?.eventId, link.unlinked?.event.id);
      }
      assert.deepStrictEqual(
        yield* sql`SELECT sequence,hex(payload_json) AS payload FROM orchestration_events ORDER BY sequence`,
        journal,
      );
      const before = yield* projectionSnapshot;
      assert.strictEqual(
        (yield* store.rebuildFoundation({ taskId: "task", throughSequence: 10 }).pipe(Effect.flip))
          .code,
        "legacy",
      );
      for (const value of events) assert.isFalse((yield* store.applyFoundation(value)).applied);
      assert.deepStrictEqual(yield* projectionSnapshot, before);
      yield* insert(yield* record(11, { taskId: "unsupported" }), 0);
      yield* insert(
        yield* record(12, { taskId: "unsupported", type: "task.archived", archived: true }),
        1,
      );
      assert.strictEqual(
        (yield* store
          .rebuildFoundation({ taskId: "unsupported", throughSequence: 12 })
          .pipe(Effect.flip)).code,
        "unsupported",
      );
      assert.isUndefined(yield* store.readFoundation("unsupported"));
      assert.deepStrictEqual(yield* projectionSnapshot, before);
      for (const [query, index] of [
        [
          "SELECT * FROM work_task_projections WHERE task_id='task'",
          "sqlite_autoindex_work_task_projections",
        ],
        [
          "SELECT * FROM work_task_current_state WHERE task_id='task'",
          "sqlite_autoindex_work_task_current_state",
        ],
        [
          "SELECT * FROM work_task_accepted_definitions WHERE task_id='task' AND definition_revision=2 ORDER BY definition_revision DESC LIMIT 1",
          "sqlite_autoindex_work_task_accepted_definitions",
        ],
        [
          "SELECT * FROM work_task_accepted_definitions WHERE task_id='task' ORDER BY definition_revision DESC LIMIT 1",
          "sqlite_autoindex_work_task_accepted_definitions",
        ],
        [
          "SELECT * FROM work_task_thread_links WHERE link_event_id='event-5' AND task_id='task'",
          "sqlite_autoindex_work_task_thread_links",
        ],
        [
          "SELECT link_event_id FROM work_task_thread_links WHERE task_id='task' AND thread_id='ordinary' AND unlink_event_id IS NULL",
          "work_task_thread_links_active_idx",
        ],
        [
          "UPDATE work_task_current_state SET last_applied_sequence=11 WHERE task_id='task' AND last_applied_sequence=10 AND EXISTS(SELECT 1 FROM work_task_projections p WHERE p.task_id=work_task_current_state.task_id AND definition_revision=2 AND lifecycle_revision=1 AND completion_policy_revision=1 AND action_policy_revision=1) RETURNING 1",
          "sqlite_autoindex_work_task_current_state",
        ],
      ]) {
        const plan = JSON.stringify(yield* sql.unsafe(`EXPLAIN QUERY PLAN ${query}`));
        assert.include(plan, index!);
        assert.include(plan, "SEARCH");
        assert.notInclude(plan, "TEMP B-TREE");
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "qualifies manager history explicitly and preserves the checked prefix across restart",
  () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* TaskStore.TaskStore;
      const created = yield* record(1);
      yield* insert(created, 0);
      yield* store.applyFoundation(created);
      assert.deepStrictEqual(
        yield* store.auditManagerReferences({ taskId: "task", expectedCheckpointSequence: 1 }),
        { eventId: "event-1", sequence: 1, streamVersion: 0 },
      );
      const sql = yield* SqlClient.SqlClient;
      const marker = yield* sql`SELECT * FROM work_task_manager_reference_checkpoints`;
      const first = yield* record(2, execution(1));
      yield* insert(first, 1);
      yield* store.applyFoundation(first);
      const collision = yield* record(3, {
        ...execution(2),
        manager: { managerThreadId: "manager-1", managerGeneration: 2 },
      });
      yield* insert(collision, 2);
      assert.strictEqual(
        (yield* store.applyFoundation(collision).pipe(Effect.flip)).code,
        "manager",
      );
      assert.strictEqual(
        (yield* sql<{
          sequence: number;
        }>`SELECT checked_sequence AS sequence FROM work_task_manager_reference_checkpoints`)[0]!
          .sequence,
        2,
      );
      assert.strictEqual(marker.length, 1);
      const seedLegacy = Effect.fn("TaskStoreTest.seedUncheckedPrefix")(function* (
        taskId: string,
        base: number,
        repeated: boolean,
      ) {
        const values = [
          yield* record(base, { taskId }),
          yield* record(base + 1, { ...execution(1), taskId }),
          yield* record(base + 2, {
            ...execution(2),
            taskId,
            manager: {
              managerThreadId: repeated ? "manager-1" : "manager-2",
              managerGeneration: 2,
            },
          }),
        ];
        for (let version = 0; version < values.length; version++)
          yield* insert(values[version]!, version);
        yield* sql`INSERT INTO work_task_projections VALUES(json_extract(${JSON.stringify(taskId)},'$'),'project',1,1,1,1,${JSON.stringify(definition)},${timestamp},${timestamp})`;
        yield* sql`INSERT INTO work_task_current_state(task_id,last_applied_sequence) VALUES(json_extract(${JSON.stringify(taskId)},'$'),${base + 2})`;
        return values;
      });
      const bad = yield* seedLegacy("bad", 100, true);
      const failed = yield* store
        .auditManagerReferences({ taskId: "bad", expectedCheckpointSequence: 102 })
        .pipe(Effect.flip);
      assert.strictEqual(failed.code, "manager");
      assert.deepStrictEqual(
        "conflictingEventIds" in failed ? failed.conflictingEventIds : undefined,
        ["event-101", "event-102"],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_manager_reference_checkpoints WHERE task_id='bad'`,
        [],
      );
      assert.isFalse((yield* store.applyFoundation(bad[0]!)).applied);
      const goodId = "\u0000good\ud800";
      yield* seedLegacy(goodId, 200, false);
      const next = yield* record(203, { ...configuration, taskId: goodId });
      yield* insert(next, 3);
      assert.strictEqual(
        (yield* store.applyFoundation(next).pipe(Effect.flip)).code,
        "qualification",
      );
      const before = yield* projectionSnapshot;
      assert.deepStrictEqual(
        yield* store.auditManagerReferences({ taskId: goodId, expectedCheckpointSequence: 202 }),
        { eventId: "event-202", sequence: 202, streamVersion: 2 },
      );
      assert.deepStrictEqual(yield* projectionSnapshot, before);
      yield* Effect.gen(function* () {
        const restarted = yield* TaskStore.TaskStore;
        yield* restarted.applyFoundation(next);
        assert.isFalse((yield* restarted.applyFoundation(next)).applied);
      }).pipe(Effect.provide(TaskStore.layer));
      assert.strictEqual(
        (yield* store
          .auditManagerReferences({ taskId: goodId, expectedCheckpointSequence: 202 })
          .pipe(Effect.flip)).code,
        "cas",
      );
      const later = yield* record(204, { ...configuration, taskId: goodId });
      yield* insert(later, 4);
      const stable = yield* projectionSnapshot;
      const stableMarkers =
        yield* sql`SELECT hex(task_id),checked_event_id,checked_sequence,checked_stream_version FROM work_task_manager_reference_checkpoints`;
      yield* sql`CREATE TEMP TRIGGER reject_reference BEFORE UPDATE ON work_task_manager_reference_checkpoints BEGIN SELECT RAISE(IGNORE); END`;
      assert.strictEqual((yield* store.applyFoundation(later).pipe(Effect.flip)).code, "cas");
      assert.deepStrictEqual(yield* projectionSnapshot, stable);
      assert.deepStrictEqual(
        yield* sql`SELECT hex(task_id),checked_event_id,checked_sequence,checked_stream_version FROM work_task_manager_reference_checkpoints`,
        stableMarkers,
      );
      yield* sql`DROP TRIGGER reject_reference`;
      yield* store.applyFoundation(later);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("checks every typed manager reference and reads actual applied debit evidence", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore.TaskStore;
    const pair = { managerThreadId: "\u0000manager\ud800", managerGeneration: 1 };
    const payloadIdentity = {
      action: "git.push",
      target: "origin/main",
      fingerprint: "sha256:payload",
    };
    const direct = {
      ...pair,
      runId: "run",
      messageId: "message",
      effectId: "effect",
      sourceIds: ["source"],
      dirtyGeneration: 1,
      claimId: "claim",
      evidenceId: "evidence",
      observedWorkSequence: 0,
      summary: "claim",
      evidence: [{ type: "artifact", uri: "artifact:proof", hash: "hash" }],
      waitId: "wait",
      target: { type: "source", sourceId: "source" },
      state: "registered",
      reviewId: "review",
      reviewRevision: 1,
      payloadIdentity,
      decision: "approve",
      human: { subject: "human", sessionId: "session" },
    };
    const types = [
      "task.execution-updated",
      "task.action-review-requested",
      "task.action-review-invalidated",
      "task.action-review-cancelled",
      "task.action-review-continuation-released",
      "task.manager-bound",
      "task.manager-retired",
      "task.wait-updated",
      "task.continuation-accepted",
      "task.completion-submitted",
      "task.completed",
      "task.action-review-result-recorded",
    ];
    for (const [index, type] of types.entries()) {
      const base = 1000 + index * 10;
      const taskId = `form-${index}`;
      const created = yield* record(base, { taskId });
      yield* insert(created, 0);
      yield* store.applyFoundation(created);
      const binding = {
        taskId,
        reviewId: "review",
        reviewRevision: 1,
        definitionRevision: 1,
        actionPolicyRevision: 1,
        payloadIdentity,
        ruleId: "rule",
        runtimeRequestId: "request",
        providerSessionId: "provider",
        ...pair,
        runId: "run",
      };
      const reference = yield* record(base + 1, {
        ...direct,
        ...execution(1),
        manager: pair,
        type,
        taskId,
        binding,
        review: {
          ...payloadIdentity,
          ...pair,
          taskId,
          reviewId: "review",
          revision: 1,
          providerSessionId: "provider",
          runId: "run",
          ruleId: "rule",
          definitionRevision: 1,
          actionPolicyRevision: 1,
          label: "Push",
          preview: { content: "Exact changes", artifacts: [] },
          state: { type: "pending" },
          createdSequence: base + 1,
          createdAt: timestamp,
          runtimeRequestId: "request",
        },
        cause: "payload_changed",
        reason: "recorded",
        source: {
          type: "task.archive",
          commandId: `command-${base + 1}`,
          human: { subject: "human", sessionId: "session" },
        },
        continuationId: "continuation",
        decisionEventId: "decision",
        purpose: "approved_action",
      });
      yield* insert(reference, 1);
      if (type === "task.execution-updated")
        yield* sql`UPDATE orchestration_events SET payload_json=json_set(payload_json,'$.binding',json(${JSON.stringify({ managerThreadId: "masked", managerGeneration: 99 })})) WHERE event_id=${reference.event.id}`;
      yield* sql`UPDATE work_task_current_state SET last_applied_sequence=${base + 1} WHERE task_id=${taskId}`;
      yield* store.auditManagerReferences({ taskId, expectedCheckpointSequence: base + 1 });
      for (const manager of [
        { managerThreadId: "different", managerGeneration: 1 },
        { ...pair, managerGeneration: 2 },
        pair,
      ]) {
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const value = yield* record(base + 2, {
                ...execution(manager.managerGeneration),
                manager,
                taskId,
              });
              yield* insert(value, 2);
              if (manager === pair) assert.isTrue((yield* store.applyFoundation(value)).applied);
              else
                assert.strictEqual(
                  (yield* store.applyFoundation(value).pipe(Effect.flip)).code,
                  "manager",
                );
              return yield* Effect.fail("fixture rollback");
            }),
          )
          .pipe(Effect.flip);
      }
    }
    const created = yield* record(2000, { taskId: "debit" });
    const configured = yield* record(2001, {
      taskId: "debit",
      type: "task.allowance-configured",
      commandId: "command-2001",
      previous: { type: "unconfigured", consumed: 0 },
      resulting: { type: "configured", cap: 2, consumed: 0 },
    });
    const debit = yield* record(2002, {
      taskId: "debit",
      type: "task.allowance-debited",
      commandId: "command-2002",
      allowanceUnit: 1,
      previous: { type: "configured", cap: 2, consumed: 0 },
      resulting: { type: "configured", cap: 2, consumed: 1 },
    });
    for (const [version, value] of [created, configured, debit].entries())
      yield* insert(value, version);
    yield* store.applyFoundation(created);
    yield* sql`UPDATE work_task_current_state SET last_applied_sequence=2002 WHERE task_id='debit'`;
    const before = yield* projectionSnapshot;
    assert.deepStrictEqual(
      (yield* store.readAllowanceDebitSource({
        taskId: "debit",
        allowanceUnit: 1,
        checkpointSequence: 2002,
      }))?.recorded,
      debit,
    );
    assert.isUndefined(
      yield* store.readAllowanceDebitSource({
        taskId: "debit",
        allowanceUnit: 2,
        checkpointSequence: 2002,
      }),
    );
    assert.strictEqual(
      (yield* store
        .readAllowanceDebitSource({ taskId: "debit", allowanceUnit: 1, checkpointSequence: 2001 })
        .pipe(Effect.flip)).code,
      "cas",
    );
    assert.strictEqual(
      (yield* store
        .readAllowanceDebitSource({ taskId: "debit", allowanceUnit: 0, checkpointSequence: 2002 })
        .pipe(Effect.flip)).code,
      "invalid",
    );
    assert.deepStrictEqual(yield* projectionSnapshot, before);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("publishes an audit only after unchanged source and checkpoint checks", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    for (const [index, mode] of ["cancel", "advance", "future", "prune"].entries()) {
      const taskId = `audit-${mode}`;
      const base = 3000 + index * 10;
      const created = yield* record(base, { taskId });
      const later = yield* record(base + 1, { ...configuration, taskId });
      yield* insert(created, 0);
      yield* sql`INSERT INTO work_task_projections VALUES(${taskId},'project',1,1,1,1,${JSON.stringify(definition)},${timestamp},${timestamp})`;
      yield* sql`INSERT INTO work_task_current_state(task_id,last_applied_sequence) VALUES(${taskId},${base})`;
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let pageRead = false;
      let armed = true;
      const observed = new Proxy(sql, {
        get(target, key, receiver) {
          if (key === "unsafe")
            return <A extends object>(query: string, parameters?: ReadonlyArray<unknown>) => {
              if (query.includes("stream_version>?") && query.includes("ORDER BY stream_version"))
                pageRead = true;
              return target.unsafe<A>(query, parameters);
            };
          if (key === "withTransaction")
            return <A, E, R>(operation: Effect.Effect<A, E, R>) =>
              target.withTransaction(operation).pipe(
                Effect.tap(() => {
                  if (!armed || !pageRead) return Effect.void;
                  armed = false;
                  return Deferred.succeed(reached, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                  );
                }),
              );
          return Reflect.get(target, key, receiver);
        },
      });
      const running = yield* Effect.gen(function* () {
        const store = yield* TaskStore.TaskStore;
        return yield* store.auditManagerReferences({ taskId, expectedCheckpointSequence: base });
      }).pipe(
        Effect.provide(TaskStore.layer),
        Effect.provideService(SqlClient.SqlClient, observed),
        Effect.forkChild,
      );
      yield* Deferred.await(reached);
      if (mode === "cancel") yield* Fiber.interrupt(running);
      else {
        yield* insert(later, 1);
        if (mode === "advance")
          yield* sql`UPDATE work_task_current_state SET last_applied_sequence=${base + 1} WHERE task_id=${taskId}`;
        if (mode === "prune") {
          yield* sql`INSERT INTO orchestration_v2_projection_metadata(projection_name,schema_version,last_sequence,updated_at)
            VALUES('event-log-pruned-through',2,${base},${timestamp})`;
          const marker = yield* sql<{
            schema_version: number;
            last_sequence: number;
          }>`SELECT schema_version,last_sequence
            FROM orchestration_v2_projection_metadata WHERE projection_name='event-log-pruned-through'`;
          assert.strictEqual(marker[0]?.schema_version, 2);
          assert.strictEqual(marker[0]?.last_sequence, base);
        }
        yield* Deferred.succeed(release, undefined);
        if (mode === "future")
          assert.deepStrictEqual(yield* Fiber.join(running), {
            eventId: `event-${base}`,
            sequence: base,
            streamVersion: 0,
          });
        else
          assert.strictEqual(
            (yield* Fiber.join(running).pipe(Effect.flip)).code,
            mode === "advance" ? "cas" : "source_unavailable",
          );
      }
      const rows =
        yield* sql`SELECT * FROM work_task_manager_reference_checkpoints WHERE task_id=${taskId}`;
      assert.strictEqual(rows.length, mode === "future" ? 1 : 0);
      assert.strictEqual(
        (yield* sql<{ generation: number }>`SELECT generation FROM work_task_view_fence`)[0]!
          .generation,
        0,
      );
    }
  }).pipe(Effect.provide(db)),
);
