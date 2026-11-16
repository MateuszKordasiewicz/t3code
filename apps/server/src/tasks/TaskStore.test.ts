import { assert, it } from "@effect/vitest";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { WorkTaskPersistedEvent } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../persistence/Migrations.ts";
import * as TaskStore from "./TaskStore.ts";

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
      occurredAt: timestamp,
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
