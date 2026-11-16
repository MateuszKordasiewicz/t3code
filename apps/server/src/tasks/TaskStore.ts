import { WorkTaskPersistedEvent } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  EVENT_LOG_PRUNED_THROUGH,
  EVENT_LOG_PRUNE_MARK,
} from "../persistence/OrchestrationEventStore.ts";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "../orchestration-v2/ProjectionStore.ts";

export class TaskSourceError extends Schema.TaggedError<TaskSourceError>()("TaskSourceError", {
  code: Schema.Literals([
    "source_unavailable",
    "source_invalid",
    "source_identity",
    "source_version",
    "source_counter",
    "source_bytes",
    "source_sql",
  ]),
  reason: Schema.String,
  taskId: Schema.optional(Schema.String),
}) {}
const isSourceError = Schema.is(TaskSourceError);
type Native = { readonly recorded: WorkTaskPersistedEvent; readonly streamVersion: number };
type Anchor = {
  readonly eventId: string;
  readonly sequence: number;
  readonly streamVersion: number;
  readonly priorPrunedThrough: number;
  readonly throughSequence: number;
};
type FoundationInput = {
  readonly taskId: string;
  readonly checkpointSequence: number | null;
  readonly incomingEventId?: string;
  readonly managerGeneration?: number;
  readonly acceptanceCommandId?: string;
};
type FoundationSource = {
  readonly creation: Native;
  readonly checkpoint: Native | undefined;
  readonly next: Native | undefined;
  readonly incoming: Native | undefined;
  readonly latestDefinition: Native | undefined;
  readonly configuration: Native | undefined;
  readonly manager: Native | undefined;
  readonly highestManager: Native | undefined;
  readonly acceptance: Native | undefined;
  readonly prunedThrough: number;
};
export class TaskStore extends Context.Service<
  TaskStore,
  {
    readonly readNativeEvent: (input: {
      readonly taskId: string;
      readonly eventId: string;
    }) => Effect.Effect<Native, TaskSourceError>;
    readonly readNativeSequence: (input: {
      readonly taskId: string;
      readonly sequence: number;
      readonly eventId?: string;
    }) => Effect.Effect<Native, TaskSourceError>;
    readonly readFoundationSource: (
      input: FoundationInput,
    ) => Effect.Effect<FoundationSource, TaskSourceError>;
    readonly readNativeHistory: (input: {
      readonly taskId: string;
      readonly throughSequence: number;
      readonly after?: Anchor | null;
      readonly limit?: number;
    }) => Effect.Effect<
      {
        readonly records: ReadonlyArray<Native>;
        readonly next: Anchor | undefined;
        readonly prunedThrough: number;
      },
      TaskSourceError
    >;
  }
>()("t3/tasks/TaskStore") {}

const invalid = (code: TaskSourceError["code"], reason: string = code): never => {
  throw new TaskSourceError({ code, reason });
};
const counter = (text: unknown, type: unknown = "integer") => {
  if (type !== "integer" || typeof text !== "string" || !/^(0|[1-9]\d*)$/.test(text))
    return invalid("source_counter");
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 0) return invalid("source_counter");
  return value;
};
const inputCounter = (value: number) =>
  counter(String(value), Number.isInteger(value) ? "integer" : "real");
const decodeHex = (hex: string) => {
  if (!/^(?:[0-9A-F]{2})*$/.test(hex)) return invalid("source_bytes");
  const bytes = Buffer.from(hex, "hex");
  const characters: string[] = [];
  for (let i = 0; i < bytes.length;) {
    const first = bytes[i++]!;
    if (first < 128) {
      characters.push(String.fromCharCode(first));
      continue;
    }
    const width =
      first >= 0xc2 && first <= 0xdf
        ? 2
        : first >= 0xe0 && first <= 0xef
          ? 3
          : first >= 0xf0 && first <= 0xf4
            ? 4
            : 0;
    if (!width || i + width - 1 > bytes.length) return invalid("source_bytes");
    let value = first & (0x7f >> width);
    for (let j = 1; j < width; j++) {
      const byte = bytes[i++]!;
      if ((byte & 0xc0) !== 0x80) return invalid("source_bytes");
      value = value * 64 + (byte & 63);
    }
    if (value < (width === 2 ? 128 : width === 3 ? 2048 : 65536) || value > 0x10ffff)
      return invalid("source_bytes");
    characters.push(String.fromCodePoint(value));
  }
  return characters.join("");
};
const decodeRecorded = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);
type Raw = {
  readonly id: string;
  readonly task: string;
  readonly kind: string;
  readonly time: string;
  readonly command: string;
  readonly command_null: number;
  readonly payload: string;
  readonly sequence_text: string;
  readonly sequence_type: string;
  readonly version: string;
  readonly version_type: string;
  readonly application_version: string;
  readonly application_version_type: string;
  readonly aggregate_kind: string;
  readonly text_types: number;
};
const select = `SELECT hex(event_id) AS id,hex(stream_id) AS task,hex(event_type) AS kind,hex(occurred_at) AS time,
 hex(command_id) AS command,command_id IS NULL AS command_null,hex(payload_json) AS payload,
 CAST(sequence AS TEXT) AS sequence_text,typeof(sequence) AS sequence_type,CAST(stream_version AS TEXT) AS version,typeof(stream_version) AS version_type,
 CAST(application_event_version AS TEXT) AS application_version,typeof(application_event_version) AS application_version_type,aggregate_kind,
 (typeof(event_id)='text' AND typeof(stream_id)='text' AND typeof(event_type)='text' AND typeof(occurred_at)='text' AND typeof(payload_json)='text' AND (command_id IS NULL OR typeof(command_id)='text')) AS text_types
 FROM orchestration_events`;
const managerExpression =
  "CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.manager.managerGeneration') END";
const managerPredicate =
  "aggregate_kind='task' AND application_event_version=2 AND event_type='task.execution-updated' AND CASE WHEN json_valid(payload_json) THEN json_type(payload_json,'$.manager') END='object'";
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const attempt = <A>(body: () => A) =>
    Effect.try({
      try: body,
      catch: (error) =>
        isSourceError(error)
          ? error
          : new TaskSourceError({ code: "source_invalid", reason: "Invalid native source" }),
    });
  const decodeRow = Effect.fn("TaskStore.decodeNativeRow")(function* (row: Raw, taskId: string) {
    const fields = yield* attempt(() =>
      [row.id, row.task, row.kind, row.time, row.command, row.payload].map(decodeHex),
    );
    const canonical = yield* sql<{
      key: number;
      value_hex: string;
    }>`SELECT key,hex(value) AS value_hex FROM json_each(${yield* attempt(() => JSON.stringify(fields))})`;
    yield* attempt(() => {
      const originals = [row.id, row.task, row.kind, row.time, row.command, row.payload];
      if (
        canonical.length !== originals.length ||
        canonical.some((r) => r.value_hex !== originals[r.key])
      )
        invalid("source_bytes", "Noncanonical native bytes");
      if (
        row.text_types !== 1 ||
        row.aggregate_kind !== "task" ||
        counter(row.application_version, row.application_version_type) !== 2
      )
        invalid("source_version");
      if (row.command_null !== 0 && row.command_null !== 1) invalid("source_identity");
    });
    const sequence = yield* attempt(() => counter(row.sequence_text, row.sequence_type));
    const streamVersion = yield* attempt(() => counter(row.version, row.version_type));
    const payload = yield* attempt(() => JSON.parse(fields[5]!));
    const recorded = yield* decodeRecorded({
      event: payload,
      context: {
        sequence,
        eventId: fields[0],
        occurredAt: fields[3],
        commandId: row.command_null === 1 ? null : fields[4],
      },
    }).pipe(
      Effect.mapError(
        () =>
          new TaskSourceError({
            code: "source_invalid",
            reason: "Native payload or context is invalid",
          }),
      ),
    );
    yield* attempt(() => {
      if (
        recorded.event.id !== fields[0] ||
        recorded.event.taskId !== fields[1] ||
        fields[1] !== taskId ||
        recorded.event.type !== fields[2] ||
        recorded.event.occurredAt !== fields[3] ||
        recorded.context.commandId !== (row.command_null === 1 ? null : fields[4])
      )
        invalid("source_identity");
    });
    return { recorded, streamVersion };
  });
  type SourceIndex =
    | "idx_task_source_type_sequence"
    | "idx_task_source_manager_sequence"
    | "idx_orch_events_command_id";
  // SQLite otherwise prefers the global application-sequence index for selective task witnesses.
  const rows = (where: string, parameters: ReadonlyArray<string | number>, index?: SourceIndex) =>
    sql.unsafe<Raw>(`${select}${index ? ` INDEXED BY ${index}` : ""} WHERE ${where}`, parameters);
  const one = Effect.fn("TaskStore.readNativeWitness")(function* (
    taskId: string,
    where: string,
    parameters: ReadonlyArray<string | number>,
    index?: SourceIndex,
  ) {
    const found = yield* rows(where, parameters, index);
    return found[0] ? yield* decodeRow(found[0], taskId) : undefined;
  });
  const required = <A>(value: A | undefined) =>
    attempt(() => (value === undefined ? invalid("source_unavailable") : value));
  const bySequence = (taskId: string, sequence: number) => one(taskId, "sequence=?", [sequence]);
  const byVersion = (taskId: string, version: number) =>
    one(taskId, "aggregate_kind='task' AND stream_id=json_extract(?,'$') AND stream_version=?", [
      JSON.stringify(taskId),
      version,
    ]);
  const headAt = (taskId: string, through: number) =>
    one(
      taskId,
      "aggregate_kind='task' AND stream_id=json_extract(?,'$') AND sequence<=? ORDER BY sequence DESC LIMIT 1",
      [JSON.stringify(taskId), through],
    );
  const coverage = Effect.fn("TaskStore.readSourceCoverage")(function* (taskId: string, prior = 0) {
    yield* attempt(() => inputCounter(prior));
    const metadata = yield* sql<{
      projection_name: string;
      last_sequence: string;
      counter_type: string;
      schema_version: string;
      schema_type: string;
      updated_at: string;
    }>`SELECT projection_name,CAST(last_sequence AS TEXT) AS last_sequence,typeof(last_sequence) AS counter_type,CAST(schema_version AS TEXT) AS schema_version,typeof(schema_version) AS schema_type,updated_at FROM orchestration_v2_projection_metadata WHERE projection_name IN (${EVENT_LOG_PRUNED_THROUGH},${EVENT_LOG_PRUNE_MARK})`;
    let prunedThrough = 0;
    for (const marker of metadata) {
      const value = yield* attempt(() => counter(marker.last_sequence, marker.counter_type));
      yield* attempt(() => {
        const time = DateTime.makeUnsafe(marker.updated_at);
        if (DateTime.formatIso(time) !== marker.updated_at)
          invalid("source_unavailable", "Invalid pruning time");
        if (
          counter(marker.schema_version, marker.schema_type) !==
          ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION
        )
          invalid("source_unavailable", "Unknown pruning schema");
      });
      if (marker.projection_name === EVENT_LOG_PRUNED_THROUGH) prunedThrough = value;
    }
    const creation = yield* byVersion(taskId, 0);
    const global = yield* sql<{
      head: string;
    }>`SELECT CAST(COALESCE(MAX(sequence),0) AS TEXT) AS head FROM orchestration_events`;
    const globalHead = yield* attempt(() => counter(global[0]!.head));
    yield* attempt(() => {
      if (
        !creation ||
        creation.recorded.event.type !== "task.created" ||
        creation.recorded.context.sequence <= prunedThrough ||
        prunedThrough < prior ||
        prunedThrough > globalHead
      )
        invalid("source_unavailable");
    });
    return { creation: yield* required(creation), prunedThrough, globalHead };
  });
  const transaction = <A, E>(taskId: string, operation: Effect.Effect<A, E>) =>
    operation.pipe(
      sql.withTransaction,
      NodeSqliteClient.readOnly,
      Effect.mapError((error) =>
        isSourceError(error)
          ? new TaskSourceError({ code: error.code, reason: error.reason, taskId })
          : new TaskSourceError({
              code: "source_sql",
              reason: "Native source query failed",
              taskId,
            }),
      ),
    );
  const readNativeEvent = Effect.fn("TaskStore.readNativeEvent")(
    (input: { taskId: string; eventId: string }) =>
      transaction(
        input.taskId,
        Effect.gen(function* () {
          yield* coverage(input.taskId);
          return yield* required(
            yield* one(input.taskId, "event_id=json_extract(?,'$')", [
              yield* attempt(() => JSON.stringify(input.eventId)),
            ]),
          );
        }),
      ),
  );
  const readNativeSequence = Effect.fn("TaskStore.readNativeSequence")(
    (input: { taskId: string; sequence: number; eventId?: string }) =>
      transaction(
        input.taskId,
        Effect.gen(function* () {
          yield* attempt(() => inputCounter(input.sequence));
          yield* coverage(input.taskId);
          const found = yield* required(yield* bySequence(input.taskId, input.sequence));
          yield* attempt(() => {
            if (input.eventId !== undefined && found.recorded.event.id !== input.eventId)
              invalid("source_identity");
          });
          return found;
        }),
      ),
  );
  const latestType = (taskId: string, type: string, through: number) =>
    one(
      taskId,
      "aggregate_kind='task' AND application_event_version=2 AND stream_id=json_extract(?,'$') AND event_type=? AND sequence<=? ORDER BY sequence DESC LIMIT 1",
      [JSON.stringify(taskId), type, through],
      "idx_task_source_type_sequence",
    );
  const readFoundationSource = Effect.fn("TaskStore.readFoundationSource")(
    (input: FoundationInput) =>
      transaction(
        input.taskId,
        Effect.gen(function* () {
          const basis = yield* coverage(input.taskId);
          if (input.checkpointSequence !== null)
            yield* attempt(() => inputCounter(input.checkpointSequence!));
          const checkpoint =
            input.checkpointSequence === null
              ? undefined
              : yield* required(yield* bySequence(input.taskId, input.checkpointSequence));
          const head = yield* required(yield* headAt(input.taskId, basis.globalHead));
          const atHead = checkpoint?.streamVersion === head.streamVersion;
          const nextVersion = atHead
            ? 0
            : yield* attempt(() => {
                if (checkpoint?.streamVersion === Number.MAX_SAFE_INTEGER)
                  invalid("source_counter");
                return checkpoint ? checkpoint.streamVersion + 1 : 0;
              });
          const next = atHead
            ? undefined
            : yield* byVersion(input.taskId, yield* attempt(() => inputCounter(nextVersion)));
          yield* attempt(() => {
            if (
              (!next && checkpoint?.streamVersion !== head.streamVersion) ||
              (next &&
                checkpoint &&
                next.recorded.context.sequence <= checkpoint.recorded.context.sequence)
            )
              invalid("source_version");
          });
          const incoming =
            input.incomingEventId === undefined
              ? undefined
              : yield* required(
                  yield* one(input.taskId, "event_id=json_extract(?,'$')", [
                    yield* attempt(() => JSON.stringify(input.incomingEventId)),
                  ]),
                );
          yield* attempt(() => {
            if (
              incoming &&
              incoming.streamVersion > (checkpoint?.streamVersion ?? -1) &&
              incoming.recorded.event.id !== next?.recorded.event.id
            )
              invalid("source_version");
          });
          const through = checkpoint?.recorded.context.sequence ?? 0;
          const draft = yield* latestType(input.taskId, "task.definition-updated", through);
          const created = checkpoint ? basis.creation : undefined;
          const configuration = yield* latestType(
            input.taskId,
            "task.configuration-updated",
            through,
          );
          const manager =
            input.managerGeneration === undefined
              ? undefined
              : yield* one(
                  input.taskId,
                  `${managerPredicate} AND stream_id=json_extract(?,'$') AND ${managerExpression}=? AND sequence<=? ORDER BY sequence DESC LIMIT 1`,
                  [
                    yield* attempt(() => JSON.stringify(input.taskId)),
                    yield* attempt(() => inputCounter(input.managerGeneration!)),
                    through,
                  ],
                  "idx_task_source_manager_sequence",
                );
          const highestManager = yield* one(
            input.taskId,
            `${managerPredicate} AND stream_id=json_extract(?,'$') AND sequence<=? ORDER BY ${managerExpression} DESC LIMIT 1`,
            [yield* attempt(() => JSON.stringify(input.taskId)), through],
            "idx_task_source_manager_sequence",
          );
          const acceptance =
            input.acceptanceCommandId === undefined
              ? undefined
              : yield* one(
                  input.taskId,
                  "command_id=json_extract(?,'$') AND aggregate_kind='task' AND application_event_version=2 AND stream_id=json_extract(?,'$') AND event_type='task.definition-accepted' AND sequence<=? ORDER BY sequence LIMIT 1",
                  [
                    yield* attempt(() => JSON.stringify(input.acceptanceCommandId)),
                    yield* attempt(() => JSON.stringify(input.taskId)),
                    through,
                  ],
                  "idx_orch_events_command_id",
                );
          return {
            creation: basis.creation,
            checkpoint,
            next,
            incoming,
            latestDefinition: draft ?? created,
            configuration,
            manager,
            highestManager,
            acceptance,
            prunedThrough: basis.prunedThrough,
          };
        }),
      ),
  );
  const readNativeHistory = Effect.fn("TaskStore.readNativeHistory")(
    (input: { taskId: string; throughSequence: number; after?: Anchor | null; limit?: number }) =>
      transaction(
        input.taskId,
        Effect.gen(function* () {
          const limit = input.limit ?? 50;
          yield* attempt(() => {
            inputCounter(input.throughSequence);
            if (!Number.isInteger(limit) || limit < 1 || limit > 100)
              invalid("source_counter", "Invalid page limit");
          });
          if (input.after) yield* attempt(() => inputCounter(input.after!.priorPrunedThrough));
          const basis = yield* coverage(
            input.taskId,
            input.after ? input.after.priorPrunedThrough : 0,
          );
          yield* attempt(() => {
            if (input.throughSequence > basis.globalHead)
              invalid("source_version", "Future history boundary");
          });
          const head = yield* required(yield* headAt(input.taskId, input.throughSequence));
          let anchor: Native | undefined;
          if (input.after) {
            yield* attempt(() => {
              inputCounter(input.after!.sequence);
              inputCounter(input.after!.streamVersion);
              inputCounter(input.after!.throughSequence);
              if (input.after!.throughSequence !== input.throughSequence)
                invalid("source_identity", "Changed history boundary");
            });
            anchor = yield* required(yield* bySequence(input.taskId, input.after.sequence));
            yield* attempt(() => {
              if (
                anchor!.recorded.event.id !== input.after!.eventId ||
                anchor!.streamVersion !== input.after!.streamVersion ||
                anchor!.recorded.context.sequence > input.throughSequence ||
                anchor!.streamVersion > head.streamVersion
              )
                invalid("source_identity");
            });
          }
          const raw = yield* rows(
            "aggregate_kind='task' AND stream_id=json_extract(?,'$') AND stream_version>? AND stream_version<=? ORDER BY stream_version LIMIT ?",
            [
              yield* attempt(() => JSON.stringify(input.taskId)),
              anchor?.streamVersion ?? -1,
              head.streamVersion,
              limit + 1,
            ],
          );
          const decoded = yield* Effect.forEach(raw, (row) => decodeRow(row, input.taskId));
          yield* attempt(() => {
            let version = anchor?.streamVersion ?? -1;
            let sequence = anchor?.recorded.context.sequence ?? -1;
            for (const item of decoded) {
              if (
                item.streamVersion !== version + 1 ||
                item.recorded.context.sequence <= sequence ||
                item.recorded.context.sequence > input.throughSequence
              )
                invalid("source_version");
              version = item.streamVersion;
              sequence = item.recorded.context.sequence;
            }
            if (decoded.length <= limit && version !== head.streamVersion)
              invalid("source_version", "Incomplete native history");
          });
          const records = decoded.slice(0, limit);
          const last = records.at(-1);
          const next =
            decoded.length > limit && last
              ? {
                  eventId: last.recorded.event.id,
                  sequence: last.recorded.context.sequence,
                  streamVersion: last.streamVersion,
                  priorPrunedThrough: basis.prunedThrough,
                  throughSequence: input.throughSequence,
                }
              : undefined;
          return { records, next, prunedThrough: basis.prunedThrough };
        }),
      ),
  );
  return TaskStore.of({
    readNativeEvent,
    readNativeSequence,
    readFoundationSource,
    readNativeHistory,
  });
});
export const layer = Layer.effect(TaskStore, make);
