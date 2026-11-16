import {
  WorkTaskPersistedEvent,
  WorkTaskDraftDefinition,
  WorkTaskAcceptedDefinition,
  WorkTaskCallerContext,
  WorkTaskCompletionPolicyOverride,
  WorkTaskAllowanceState,
  WorkTaskId,
  ProjectId,
  ThreadId,
  EventId,
} from "@t3tools/contracts";
import * as NodeUtil from "node:util";
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
type CheckedReference = {
  readonly eventId: string;
  readonly sequence: number;
  readonly streamVersion: number;
};
const referencePaths = [
  ["manager", ["task.execution-updated"]],
  ["review", ["task.action-review-requested"]],
  [
    "binding",
    [
      "task.action-review-invalidated",
      "task.action-review-cancelled",
      "task.action-review-continuation-released",
    ],
  ],
  [
    "",
    [
      "task.manager-bound",
      "task.manager-retired",
      "task.wait-updated",
      "task.continuation-accepted",
      "task.completion-submitted",
      "task.completed",
      "task.action-review-result-recorded",
    ],
  ],
] as const;
const referencePredicate = `aggregate_kind='task' AND application_event_version=2 AND event_type IN (${referencePaths
  .flatMap(([, names]) => names)
  .map((name) => `'${name}'`)
  .join(",")})`;
const referenceExpression = (field: "managerThreadId" | "managerGeneration") =>
  `CASE WHEN json_valid(payload_json) THEN CASE ${referencePaths.map(([path, names]) => `WHEN event_type IN (${names.map((type) => `'${type}'`).join(",")}) THEN json_extract(payload_json,'$.${path ? path + "." : ""}${field}')`).join(" ")} END END`;
const referencePair = (event: WorkTaskPersistedEvent["event"]) =>
  "binding" in event
    ? event.binding
    : "review" in event
      ? event.review
      : "managerThreadId" in event
        ? event
        : "manager" in event
          ? event.manager
          : null;
const checkedReference = (value: Native): CheckedReference => ({
  eventId: value.recorded.event.id,
  sequence: value.recorded.context.sequence,
  streamVersion: value.streamVersion,
});
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
export class TaskProjectionError extends Schema.TaggedError<TaskProjectionError>()(
  "TaskProjectionError",
  {
    code: Schema.Literals([
      "invalid",
      "unsupported",
      "legacy",
      "replay",
      "revision",
      "definition",
      "policy",
      "manager",
      "link",
      "cas",
      "fence",
      "qualification",
      "allowance",
      "command",
      "unit",
      "sql",
    ]),
    conflictingEventIds: Schema.optional(Schema.Array(Schema.String)),
  },
) {}
const isProjectionError = Schema.is(TaskProjectionError);
const natural = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const positive = natural.check(Schema.isGreaterThan(0));
const ReferenceCheckpoint = Schema.Struct({
  taskId: WorkTaskId,
  creationEventId: Schema.String,
  creationSequence: natural,
  eventId: Schema.String,
  sequence: natural,
  streamVersion: natural,
});
const revisions = {
  definitionRevision: positive,
  lifecycleRevision: positive,
  completionPolicyRevision: positive,
  actionPolicyRevision: positive,
};
const Configuration = Schema.Struct({
  allowanceOverride: Schema.NullOr(positive),
  completionPolicy: WorkTaskCompletionPolicyOverride,
});
const Projection = Schema.Struct({
  taskId: WorkTaskId,
  projectId: Schema.NullOr(ProjectId),
  ...revisions,
  definition: WorkTaskDraftDefinition,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  current: Schema.NullOr(
    Schema.Struct({
      configuration: Schema.NullOr(Configuration),
      desiredMode: Schema.NullOr(Schema.Literals(["not_started", "active", "paused", "closed"])),
      executionMode: Schema.NullOr(
        Schema.Literals(["not_started", "idle", "queued", "running", "waiting"]),
      ),
      managerThreadId: Schema.NullOr(ThreadId),
      managerGeneration: Schema.NullOr(positive),
      attention: Schema.Unknown,
      completion: Schema.NullOr(
        Schema.Literals(["incomplete", "ready_for_review", "validating", "done"]),
      ),
      archived: Schema.NullOr(Schema.Literals([0, 1])),
      lastSequence: Schema.NullOr(natural),
    }),
  ),
});
const Accepted = Schema.Struct({
  taskId: WorkTaskId,
  definitionRevision: positive,
  actionPolicyRevision: positive,
  definition: WorkTaskAcceptedDefinition,
  attribution: WorkTaskCallerContext,
  eventId: EventId,
  occurredAt: Schema.String,
});
const Link = Schema.Struct({
  taskId: WorkTaskId,
  threadId: ThreadId,
  eventId: EventId,
  occurredAt: Schema.String,
  attribution: WorkTaskCallerContext,
  unlinked: Schema.NullOr(
    Schema.Struct({
      eventId: EventId,
      occurredAt: Schema.String,
      attribution: WorkTaskCallerContext,
    }),
  ),
});
type ProjectionRead = {
  readonly projection: typeof Projection.Type;
  readonly sourceVerified: boolean;
  readonly draft: WorkTaskDraftDefinition | undefined;
  readonly accepted: typeof Accepted.Type | undefined;
};
type ProjectionFailure = TaskProjectionError | TaskSourceError;
const ManagerBinding = Schema.Struct({
  taskId: WorkTaskId,
  managerGeneration: positive,
  managerThreadId: ThreadId,
  boundAt: Schema.String,
  retiredAt: Schema.NullOr(Schema.String),
});
type ManagerRead = typeof ManagerBinding.Type & {
  readonly bound: Native;
  readonly retired: Native | undefined;
};
type CustodyRead = {
  readonly current: ManagerRead | undefined;
  readonly highestGeneration: number;
};
type AllowanceRead = {
  readonly allowance: WorkTaskAllowanceState;
  readonly source: Native | undefined;
};
const allowanceTypes = [
  "task.allowance-configured",
  "task.allowance-topped-up",
  "task.allowance-debited",
] as const;
const custodyTypes = ["task.manager-bound", "task.manager-retired", ...allowanceTypes];

export class TaskStore extends Context.Service<
  TaskStore,
  {
    readonly applyManagerAllowance: (
      recorded: WorkTaskPersistedEvent,
    ) => Effect.Effect<
      { readonly applied: boolean; readonly lastSequence: number; readonly viewGeneration: number },
      ProjectionFailure
    >;
    readonly readManagerCustody: (
      taskId: string,
    ) => Effect.Effect<CustodyRead | undefined, ProjectionFailure>;
    readonly readManagerBinding: (input: {
      readonly taskId: string;
      readonly generation: number;
    }) => Effect.Effect<ManagerRead | undefined, ProjectionFailure>;
    readonly readAllowance: (
      taskId: string,
    ) => Effect.Effect<AllowanceRead | undefined, ProjectionFailure>;
    readonly auditManagerReferences: (input: {
      readonly taskId: string;
      readonly expectedCheckpointSequence: number;
    }) => Effect.Effect<CheckedReference, ProjectionFailure>;
    readonly readAllowanceDebitSource: (input: {
      readonly taskId: string;
      readonly allowanceUnit: number;
      readonly checkpointSequence: number;
    }) => Effect.Effect<Native | undefined, ProjectionFailure>;
    readonly applyFoundation: (
      recorded: WorkTaskPersistedEvent,
    ) => Effect.Effect<
      { readonly applied: boolean; readonly lastSequence: number; readonly viewGeneration: number },
      ProjectionFailure
    >;
    readonly readFoundation: (
      taskId: string,
    ) => Effect.Effect<ProjectionRead | undefined, ProjectionFailure>;
    readonly readAcceptedDefinition: (input: {
      readonly taskId: string;
      readonly revision?: number;
    }) => Effect.Effect<typeof Accepted.Type | undefined, ProjectionFailure>;
    readonly readThreadLink: (input: {
      readonly taskId: string;
      readonly linkEventId: string;
    }) => Effect.Effect<typeof Link.Type | undefined, ProjectionFailure>;
    readonly rebuildFoundation: (input: {
      readonly taskId: string;
      readonly throughSequence: number;
    }) => Effect.Effect<ProjectionRead | undefined, ProjectionFailure>;
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
    | "idx_task_reference_thread"
    | "idx_task_reference_generation"
    | "idx_task_allowance_debit_unit"
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
  const encodeProjectionJson = Effect.fn("TaskStore.encodeProjectionJson")(
    Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown)),
    Effect.mapError(() => new TaskProjectionError({ code: "invalid" })),
  );
  const guard = (condition: boolean, code: TaskProjectionError["code"]) =>
    condition ? Effect.void : Effect.fail(new TaskProjectionError({ code }));
  const projectTransaction = <A, E>(operation: Effect.Effect<A, E>) =>
    operation.pipe(
      sql.withTransaction,
      Effect.mapError((error) =>
        isSourceError(error) || isProjectionError(error)
          ? error
          : new TaskProjectionError({ code: "sql" }),
      ),
    );
  const stored = Effect.fn("TaskStore.decodeProjection")(function* <A>(
    schema: Schema.Codec<A, unknown>,
    row: { payload: string; valid: number } | undefined,
  ) {
    if (!row) return undefined;
    yield* guard(row.valid === 1, "invalid");
    const value = yield* attempt(() => JSON.parse(decodeHex(row.payload))).pipe(
      Effect.mapError(() => new TaskProjectionError({ code: "invalid" })),
    );
    const decoded = yield* Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError(() => new TaskProjectionError({ code: "invalid" })),
    );
    yield* guard(NodeUtil.isDeepStrictEqual(value, decoded), "invalid");
    const atoms = yield* sql<{ side: number; key: string; kind: string; atom: string | null }>`
        SELECT 0 AS side,hex(fullkey) AS key,CASE WHEN type IN ('integer','real') THEN 'number' ELSE type END AS kind,
          CASE WHEN type='text' THEN hex(atom) END AS atom FROM json_tree(CAST(unhex(${row.payload}) AS TEXT))
        UNION ALL SELECT 1,hex(fullkey),CASE WHEN type IN ('integer','real') THEN 'number' ELSE type END,
          CASE WHEN type='text' THEN hex(atom) END FROM json_tree(${yield* encodeProjectionJson(decoded)})`;
    const signature = (side: number) =>
      atoms
        .filter((item) => item.side === side)
        .map(({ key, kind, atom }) => JSON.stringify([key, kind, atom]))
        .sort();
    yield* guard(NodeUtil.isDeepStrictEqual(signature(0), signature(1)), "invalid");
    return decoded;
  });
  const readProjection = Effect.fn("TaskStore.readProjectionRow")(function* (taskId: string) {
    const found = yield* sql<{ payload: string; valid: number }>`SELECT
      typeof(definition_revision)='integer' AND typeof(lifecycle_revision)='integer' AND typeof(completion_policy_revision)='integer' AND typeof(action_policy_revision)='integer'
      AND NOT EXISTS(SELECT 1 FROM work_task_current_state c WHERE c.task_id=p.task_id AND
        ((c.manager_generation IS NOT NULL AND typeof(c.manager_generation)<>'integer') OR (c.last_applied_sequence IS NOT NULL AND typeof(c.last_applied_sequence)<>'integer') OR (c.archived IS NOT NULL AND typeof(c.archived)<>'integer'))) AS valid,
      hex(json_object('taskId',task_id,'projectId',project_id,'definitionRevision',definition_revision,'lifecycleRevision',lifecycle_revision,
        'completionPolicyRevision',completion_policy_revision,'actionPolicyRevision',action_policy_revision,'definition',json(definition_json),'createdAt',created_at,'updatedAt',updated_at,
        'current',json((SELECT json_object('configuration',json(configuration_json),'desiredMode',desired_mode,'executionMode',execution_mode,'managerThreadId',manager_thread_id,
          'managerGeneration',manager_generation,'attention',json(attention_json),'completion',completion,'archived',archived,'lastSequence',last_applied_sequence)
          FROM work_task_current_state c WHERE c.task_id=p.task_id)))) AS payload
      FROM work_task_projections p WHERE task_id=json_extract(${yield* encodeProjectionJson(taskId)},'$')`;
    const value = yield* stored(Projection, found[0]);
    if (value)
      yield* guard(
        value.taskId === taskId &&
          (value.current?.managerThreadId == null) === (value.current?.managerGeneration == null),
        "invalid",
      );
    return value;
  });
  const acceptedRow = Effect.fn("TaskStore.readAcceptedRow")(function* (input: {
    taskId: string;
    revision?: number;
  }) {
    if (input.revision !== undefined)
      yield* guard(Number.isSafeInteger(input.revision) && input.revision > 0, "invalid");
    const found = yield* sql<{
      payload: string;
      valid: number;
    }>`SELECT typeof(definition_revision)='integer' AND typeof(action_policy_revision)='integer' AS valid,
      hex(json_object('taskId',task_id,'definitionRevision',definition_revision,'actionPolicyRevision',action_policy_revision,'definition',json(definition_json),
        'attribution',json(attribution_json),'eventId',accepted_event_id,'occurredAt',accepted_at)) AS payload
      FROM work_task_accepted_definitions WHERE task_id=json_extract(${yield* encodeProjectionJson(input.taskId)},'$')
      ${input.revision === undefined ? sql.literal("") : sql`AND definition_revision=${input.revision}`} ORDER BY definition_revision DESC LIMIT 1`;
    const value = yield* stored(Accepted, found[0]);
    if (value)
      yield* guard(
        value.taskId === input.taskId && value.definitionRevision === value.definition.revision,
        "invalid",
      );
    return value;
  });
  const linkRow = Effect.fn("TaskStore.readLinkRow")(function* (input: {
    taskId: string;
    linkEventId: string;
  }) {
    const found = yield* sql<{ payload: string; valid: number }>`SELECT 1 AS valid,
      hex(json_object('taskId',task_id,'threadId',thread_id,'eventId',link_event_id,'occurredAt',linked_at,'attribution',json(linked_attribution_json),
        'unlinked',CASE WHEN unlink_event_id IS NULL THEN NULL ELSE json_object('eventId',unlink_event_id,'occurredAt',unlinked_at,'attribution',json(unlinked_attribution_json)) END)) AS payload
      FROM work_task_thread_links WHERE link_event_id=json_extract(${yield* encodeProjectionJson(input.linkEventId)},'$') AND task_id=json_extract(${yield* encodeProjectionJson(input.taskId)},'$')`;
    return yield* stored(Link, found[0]);
  });
  const readAcceptedDefinition = Effect.fn("TaskStore.readAcceptedDefinition")(
    (input: { taskId: string; revision?: number }) =>
      projectTransaction(acceptedRow(input)).pipe(NodeSqliteClient.readOnly),
  );
  const readThreadLink = Effect.fn("TaskStore.readThreadLink")(
    (input: { taskId: string; linkEventId: string }) =>
      projectTransaction(linkRow(input)).pipe(NodeSqliteClient.readOnly),
  );
  const readFoundation = Effect.fn("TaskStore.readFoundation")((taskId: string) =>
    projectTransaction(
      Effect.gen(function* () {
        const projection = yield* readProjection(taskId);
        if (!projection) return undefined;
        const accepted = yield* acceptedRow({ taskId });
        const sequence = projection.current?.lastSequence;
        if (sequence == null)
          return { projection, accepted, sourceVerified: false, draft: undefined };
        const source = yield* readFoundationSource({ taskId, checkpointSequence: sequence });
        const draftEvent = source.latestDefinition?.recorded.event;
        const draft = draftEvent && "definition" in draftEvent ? draftEvent.definition : undefined;
        return { projection, accepted, sourceVerified: true, draft };
      }),
    ).pipe(NodeSqliteClient.readOnly),
  );
  const fence = Effect.fn("TaskStore.readViewFence")(function* () {
    const found = yield* sql<{
      generation: string;
      high: string;
      valid: number;
    }>`SELECT CAST(generation AS TEXT) AS generation,CAST(high_water_sequence AS TEXT) AS high,
      typeof(generation)='integer' AND typeof(high_water_sequence)='integer' AS valid FROM work_task_view_fence WHERE singleton_id=1`;
    yield* guard(found.length === 1 && found[0]?.valid === 1, "fence");
    return yield* attempt(() => ({
      generation: counter(found[0]!.generation),
      high: counter(found[0]!.high),
    })).pipe(Effect.mapError(() => new TaskProjectionError({ code: "fence" })));
  });
  const referenceCheckpoint = Effect.fn("TaskStore.readReferenceCheckpoint")(function* (
    taskId: string,
  ) {
    const found = yield* sql<{ payload: string; valid: number }>`SELECT
      typeof(creation_sequence)='integer' AND typeof(checked_sequence)='integer' AND typeof(checked_stream_version)='integer' AS valid,
      hex(json_object('taskId',task_id,'creationEventId',creation_event_id,'creationSequence',creation_sequence,
        'eventId',checked_event_id,'sequence',checked_sequence,'streamVersion',checked_stream_version)) AS payload
      FROM work_task_manager_reference_checkpoints WHERE task_id=json_extract(${yield* encodeProjectionJson(taskId)},'$')`;
    const value = yield* stored(ReferenceCheckpoint, found[0]);
    if (value) yield* guard(value.taskId === taskId, "qualification");
    return value;
  });
  const publishReferenceCheckpoint = Effect.fn("TaskStore.publishReferenceCheckpoint")(function* (
    creation: Native,
    next: Native,
    previous: typeof ReferenceCheckpoint.Type | undefined,
  ) {
    const taskId = next.recorded.event.taskId;
    if (previous)
      yield* guard(
        previous.creationEventId === creation.recorded.event.id &&
          previous.creationSequence === creation.recorded.context.sequence,
        "qualification",
      );
    const id = yield* encodeProjectionJson(taskId);
    const eventId = yield* encodeProjectionJson(next.recorded.event.id);
    const sequence = next.recorded.context.sequence;
    const changed = previous
      ? yield* sql`UPDATE work_task_manager_reference_checkpoints SET checked_event_id=json_extract(${eventId},'$'),checked_sequence=${sequence},checked_stream_version=${next.streamVersion}
        WHERE task_id=json_extract(${id},'$') AND creation_event_id=json_extract(${yield* encodeProjectionJson(previous.creationEventId)},'$') AND creation_sequence=${previous.creationSequence}
          AND checked_event_id=json_extract(${yield* encodeProjectionJson(previous.eventId)},'$') AND checked_sequence=${previous.sequence} AND checked_stream_version=${previous.streamVersion}
          AND EXISTS(SELECT 1 FROM work_task_current_state WHERE task_id=json_extract(${id},'$') AND last_applied_sequence=${sequence}) RETURNING 1`
      : yield* sql`INSERT INTO work_task_manager_reference_checkpoints(task_id,creation_event_id,creation_sequence,checked_event_id,checked_sequence,checked_stream_version)
        SELECT json_extract(${id},'$'),json_extract(${yield* encodeProjectionJson(creation.recorded.event.id)},'$'),${creation.recorded.context.sequence},json_extract(${eventId},'$'),${sequence},${next.streamVersion}
        WHERE EXISTS(SELECT 1 FROM work_task_current_state WHERE task_id=json_extract(${id},'$') AND last_applied_sequence=${sequence}) RETURNING 1`;
    yield* guard(changed.length === 1, "cas");
  });
  const checkReference = Effect.fn("TaskStore.checkManagerReference")(function* (
    event: WorkTaskPersistedEvent["event"],
    through: number,
  ) {
    const pair = referencePair(event);
    if (!pair) return;
    for (const [field, index] of [
      ["managerThreadId", "thread"],
      ["managerGeneration", "generation"],
    ] as const) {
      const known = yield* one(
        event.taskId,
        `${referencePredicate} AND stream_id=json_extract(?,'$') AND ${referenceExpression(field)}=json_extract(?,'$') AND sequence<=? ORDER BY sequence DESC LIMIT 1`,
        [
          yield* encodeProjectionJson(event.taskId),
          yield* encodeProjectionJson(pair[field]),
          through,
        ],
        `idx_task_reference_${index}`,
      );
      if (known) {
        const previous = referencePair(known.recorded.event);
        yield* guard(
          previous?.managerThreadId === pair.managerThreadId &&
            previous.managerGeneration === pair.managerGeneration,
          "manager",
        );
      }
    }
  });
  const auditManagerReferences = Effect.fn("TaskStore.auditManagerReferences")(function* (input: {
    taskId: string;
    expectedCheckpointSequence: number;
  }) {
    yield* guard(
      Number.isSafeInteger(input.expectedCheckpointSequence) &&
        input.expectedCheckpointSequence >= 0,
      "invalid",
    );
    const captured = yield* projectTransaction(
      Effect.gen(function* () {
        const current = yield* readProjection(input.taskId);
        yield* guard(current?.current?.lastSequence === input.expectedCheckpointSequence, "cas");
        return {
          source: yield* readFoundationSource({
            taskId: input.taskId,
            checkpointSequence: input.expectedCheckpointSequence,
          }),
          marker: yield* referenceCheckpoint(input.taskId),
        };
      }),
    ).pipe(NodeSqliteClient.readOnly);
    const threads = new Map<string, { generation: number; eventId: string }>();
    const generations = new Map<number, { threadId: string; eventId: string }>();
    let after: Anchor | undefined;
    do {
      const page = yield* readNativeHistory({
        taskId: input.taskId,
        throughSequence: input.expectedCheckpointSequence,
        ...(after ? { after } : {}),
      });
      for (const value of page.records) {
        const pair = referencePair(value.recorded.event);
        if (!pair) continue;
        const thread = threads.get(pair.managerThreadId);
        const generation = generations.get(pair.managerGeneration);
        const conflict =
          thread && thread.generation !== pair.managerGeneration
            ? thread
            : generation && generation.threadId !== pair.managerThreadId
              ? generation
              : undefined;
        if (conflict)
          return yield* new TaskProjectionError({
            code: "manager",
            conflictingEventIds: [conflict.eventId, value.recorded.event.id],
          });
        threads.set(pair.managerThreadId, {
          generation: pair.managerGeneration,
          eventId: value.recorded.event.id,
        });
        generations.set(pair.managerGeneration, {
          threadId: pair.managerThreadId,
          eventId: value.recorded.event.id,
        });
      }
      after = page.next;
    } while (after);
    return yield* projectTransaction(
      Effect.gen(function* () {
        const current = yield* readProjection(input.taskId);
        yield* guard(current?.current?.lastSequence === input.expectedCheckpointSequence, "cas");
        yield* coverage(input.taskId, captured.source.prunedThrough);
        const source = yield* readFoundationSource({
          taskId: input.taskId,
          checkpointSequence: input.expectedCheckpointSequence,
        });
        const marker = yield* referenceCheckpoint(input.taskId);
        yield* guard(
          NodeUtil.isDeepStrictEqual(source.creation, captured.source.creation) &&
            NodeUtil.isDeepStrictEqual(source.checkpoint, captured.source.checkpoint) &&
            NodeUtil.isDeepStrictEqual(marker, captured.marker),
          "cas",
        );
        yield* guard(source.checkpoint !== undefined, "qualification");
        yield* publishReferenceCheckpoint(source.creation, source.checkpoint!, marker);
        return checkedReference(source.checkpoint!);
      }),
    );
  });
  const readAllowanceDebitSource = Effect.fn("TaskStore.readAllowanceDebitSource")(
    (input: { taskId: string; allowanceUnit: number; checkpointSequence: number }) =>
      projectTransaction(
        Effect.gen(function* () {
          yield* guard(
            Number.isSafeInteger(input.allowanceUnit) && input.allowanceUnit > 0,
            "invalid",
          );
          const current = yield* readProjection(input.taskId);
          yield* guard(current?.current?.lastSequence === input.checkpointSequence, "cas");
          yield* readFoundationSource({
            taskId: input.taskId,
            checkpointSequence: input.checkpointSequence,
          });
          const value = yield* one(
            input.taskId,
            "aggregate_kind='task' AND application_event_version=2 AND event_type='task.allowance-debited' AND CASE WHEN json_valid(payload_json) THEN json_type(payload_json,'$.allowanceUnit') END='integer' AND stream_id=json_extract(?,'$') AND CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.allowanceUnit') END=? AND sequence<=? ORDER BY sequence DESC LIMIT 1",
            [
              yield* encodeProjectionJson(input.taskId),
              input.allowanceUnit,
              input.checkpointSequence,
            ],
            "idx_task_allowance_debit_unit",
          );
          if (value)
            yield* guard(
              value.recorded.event.type === "task.allowance-debited" &&
                value.recorded.event.allowanceUnit === input.allowanceUnit,
              "invalid",
            );
          return value;
        }),
      ).pipe(NodeSqliteClient.readOnly),
  );
  const bindingRow = Effect.fn("TaskStore.readBindingRow")(function* (
    taskId: string,
    generation?: number,
    current = false,
  ) {
    const found = yield* sql<{
      payload: string;
      valid: number;
    }>`SELECT typeof(generation)='integer' AS valid,
      hex(json_object('taskId',task_id,'managerGeneration',generation,'managerThreadId',thread_id,'boundAt',bound_at,'retiredAt',retired_at)) AS payload
      FROM work_task_manager_bindings WHERE task_id=json_extract(${yield* encodeProjectionJson(taskId)},'$')
      ${generation === undefined ? (current ? sql.literal("AND retired_at IS NULL") : sql.literal("")) : sql`AND generation=${generation}`}
      ORDER BY generation DESC LIMIT 1`;
    const value = yield* stored(ManagerBinding, found[0]);
    if (value) yield* guard(value.taskId === taskId, "legacy");
    return value;
  });
  const managerSource = (taskId: string, generation: number, type: string, through: number) =>
    one(
      taskId,
      `${referencePredicate} AND stream_id=json_extract(?,'$') AND ${referenceExpression("managerGeneration")}=? AND event_type=? AND sequence<=? ORDER BY sequence DESC LIMIT 1`,
      [JSON.stringify(taskId), generation, type, through],
      "idx_task_reference_generation",
    );
  const bindingAt = Effect.fn("TaskStore.readBindingAt")(function* (
    taskId: string,
    generation: number,
    through: number,
  ) {
    yield* guard(Number.isSafeInteger(generation) && generation > 0, "invalid");
    const row = yield* bindingRow(taskId, generation);
    const bound = yield* managerSource(taskId, generation, "task.manager-bound", through);
    const retired = yield* managerSource(taskId, generation, "task.manager-retired", through);
    if (!row && !bound && !retired) return undefined;
    yield* guard(
      row !== undefined && bound?.recorded.event.type === "task.manager-bound",
      "legacy",
    );
    const birth = bound!.recorded.event;
    yield* guard(
      "managerThreadId" in birth &&
        row!.managerThreadId === birth.managerThreadId &&
        row!.boundAt === birth.occurredAt,
      "legacy",
    );
    if (retired) {
      const end = retired.recorded.event;
      yield* guard(
        end.type === "task.manager-retired" &&
          end.managerThreadId === row!.managerThreadId &&
          retired.recorded.context.sequence > bound!.recorded.context.sequence &&
          row!.retiredAt === end.occurredAt,
        "legacy",
      );
    } else yield* guard(row!.retiredAt === null, "legacy");
    return { ...row!, bound: bound!, retired };
  });
  const custodyAt = Effect.fn("TaskStore.readCustodyAt")(function* (
    taskId: string,
    through: number,
  ) {
    const highest = yield* bindingRow(taskId);
    const current = yield* bindingRow(taskId, undefined, true);
    const birth = yield* latestType(taskId, "task.manager-bound", through);
    const retirement = yield* latestType(taskId, "task.manager-retired", through);
    if (!highest && !current && !birth && !retirement)
      return { current: undefined, highestGeneration: 0 };
    yield* guard(
      highest !== undefined && birth?.recorded.event.type === "task.manager-bound",
      "legacy",
    );
    const event = birth!.recorded.event;
    yield* guard(
      "managerGeneration" in event && event.managerGeneration === highest!.managerGeneration,
      "legacy",
    );
    const checked = yield* bindingAt(taskId, highest!.managerGeneration, through);
    yield* guard(checked !== undefined, "legacy");
    if (retirement && retirement.recorded.context.sequence > birth!.recorded.context.sequence)
      yield* guard(
        retirement.recorded.event.type === "task.manager-retired" &&
          retirement.recorded.event.managerGeneration === highest!.managerGeneration,
        "legacy",
      );
    yield* guard(
      checked!.retiredAt === null
        ? NodeUtil.isDeepStrictEqual(current, highest)
        : current === undefined,
      "legacy",
    );
    return {
      current: checked!.retiredAt === null ? checked : undefined,
      highestGeneration: highest!.managerGeneration,
    };
  });
  const allowanceAt = Effect.fn("TaskStore.readAllowanceAt")(function* (
    taskId: string,
    through: number,
  ) {
    const found = yield* sql<{
      payload: string;
      valid: number;
    }>`SELECT typeof(configured_cap)='integer' AND typeof(consumed)='integer' AS valid,
      hex(json_object('type','configured','cap',configured_cap,'consumed',consumed)) AS payload FROM work_task_allowances WHERE task_id=json_extract(${yield* encodeProjectionJson(taskId)},'$')`;
    const allowance = yield* stored(WorkTaskAllowanceState, found[0]);
    let source: Native | undefined;
    for (const type of allowanceTypes) {
      const next = yield* latestType(taskId, type, through);
      if (next && (!source || next.recorded.context.sequence > source.recorded.context.sequence))
        source = next;
    }
    if (!allowance && !source)
      return { allowance: { type: "unconfigured" as const, consumed: 0 }, source: undefined };
    const event = source?.recorded.event;
    yield* guard(
      allowance !== undefined &&
        event !== undefined &&
        "resulting" in event &&
        NodeUtil.isDeepStrictEqual(allowance, event.resulting),
      "legacy",
    );
    return { allowance: allowance!, source };
  });
  const projectedCheckpoint = Effect.fn("TaskStore.readProjectedCheckpoint")(function* (
    taskId: string,
  ) {
    const current = yield* readProjection(taskId);
    if (!current) return undefined;
    yield* guard(current.current?.lastSequence != null, "legacy");
    const sequence = current.current!.lastSequence!;
    yield* readFoundationSource({ taskId, checkpointSequence: sequence });
    return sequence;
  });
  const readManagerCustody = Effect.fn("TaskStore.readManagerCustody")((taskId: string) =>
    projectTransaction(
      Effect.gen(function* () {
        const sequence = yield* projectedCheckpoint(taskId);
        return sequence === undefined ? undefined : yield* custodyAt(taskId, sequence);
      }),
    ).pipe(NodeSqliteClient.readOnly),
  );
  const readManagerBinding = Effect.fn("TaskStore.readManagerBinding")(
    (input: { taskId: string; generation: number }) =>
      projectTransaction(
        Effect.gen(function* () {
          const sequence = yield* projectedCheckpoint(input.taskId);
          return sequence === undefined
            ? undefined
            : yield* bindingAt(input.taskId, input.generation, sequence);
        }),
      ).pipe(NodeSqliteClient.readOnly),
  );
  const readAllowance = Effect.fn("TaskStore.readAllowance")((taskId: string) =>
    projectTransaction(
      Effect.gen(function* () {
        const sequence = yield* projectedCheckpoint(taskId);
        return sequence === undefined ? undefined : yield* allowanceAt(taskId, sequence);
      }),
    ).pipe(NodeSqliteClient.readOnly),
  );
  const applyProjection = Effect.fn("TaskStore.applyProjection")(
    (input: WorkTaskPersistedEvent, family: "foundation" | "managerAllowance") =>
      projectTransaction(
        Effect.gen(function* () {
          const recorded = yield* decodeRecorded(input).pipe(
            Effect.mapError(() => new TaskProjectionError({ code: "invalid" })),
          );
          const { event, context } = recorded;
          yield* guard(
            (family === "managerAllowance"
              ? custodyTypes
              : [
                  "task.created",
                  "task.definition-updated",
                  "task.definition-accepted",
                  "task.configuration-updated",
                  "task.execution-updated",
                  "task.thread-linked",
                  "task.thread-unlinked",
                ]
            ).includes(event.type),
            "unsupported",
          );
          const original = yield* readNativeEvent({ taskId: event.taskId, eventId: event.id });
          yield* guard(NodeUtil.isDeepStrictEqual(original.recorded, recorded), "replay");
          const current = yield* readProjection(event.taskId);
          if (current) yield* guard(current.current?.lastSequence != null, "legacy");
          const checkpoint = current?.current?.lastSequence ?? null;
          const source = yield* readFoundationSource({
            taskId: event.taskId,
            checkpointSequence: checkpoint,
            incomingEventId: event.id,
            ...(event.type === "task.execution-updated" && event.manager
              ? { managerGeneration: event.manager.managerGeneration }
              : {}),
            ...(event.type === "task.definition-accepted" && context.commandId !== null
              ? { acceptanceCommandId: context.commandId }
              : {}),
          });
          const view = yield* fence();
          if (source.checkpoint && original.streamVersion <= source.checkpoint.streamVersion) {
            if (family === "managerAllowance") {
              if (event.type === "task.manager-bound" || event.type === "task.manager-retired") {
                yield* custodyAt(event.taskId, checkpoint!);
                const binding = yield* bindingAt(
                  event.taskId,
                  event.managerGeneration,
                  checkpoint!,
                );
                yield* guard(
                  binding !== undefined &&
                    (event.type !== "task.manager-retired" || binding.retiredAt !== null),
                  "legacy",
                );
              } else yield* allowanceAt(event.taskId, checkpoint!);
            }
            return { applied: false, lastSequence: checkpoint!, viewGeneration: view.generation };
          }
          const checked = yield* referenceCheckpoint(event.taskId);
          if (source.checkpoint) {
            yield* guard(
              checked !== undefined &&
                checked.creationEventId === source.creation.recorded.event.id &&
                checked.creationSequence === source.creation.recorded.context.sequence &&
                checked.eventId === source.checkpoint.recorded.event.id &&
                checked.sequence === checkpoint &&
                checked.streamVersion === source.checkpoint.streamVersion,
              "qualification",
            );
            yield* checkReference(event, checkpoint!);
          } else yield* guard(checked === undefined, "qualification");
          yield* guard(source.next?.recorded.event.id === event.id, "replay");
          yield* guard(view.generation < Number.MAX_SAFE_INTEGER, "fence");
          const keys = [
            "definitionRevision",
            "lifecycleRevision",
            "completionPolicyRevision",
            "actionPolicyRevision",
          ] as const;
          if (current) for (const key of keys) yield* guard(event[key] >= current[key], "revision");
          yield* guard(
            current ? event.type !== "task.created" : event.type === "task.created",
            "replay",
          );
          let effective = current?.definition;
          let activeLink: string | undefined;
          switch (event.type) {
            case "task.manager-bound":
            case "task.manager-retired": {
              const custody = yield* custodyAt(event.taskId, checkpoint!);
              if (event.type === "task.manager-bound") {
                yield* guard(
                  !custody.current && event.managerGeneration > custody.highestGeneration,
                  "manager",
                );
                const reused =
                  yield* sql`SELECT 1 FROM work_task_manager_bindings WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND thread_id=json_extract(${yield* encodeProjectionJson(event.managerThreadId)},'$') LIMIT 1`;
                yield* guard(reused.length === 0, "manager");
                yield* sql`INSERT INTO work_task_manager_bindings(task_id,generation,thread_id,bound_at,retired_at) VALUES(json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),${event.managerGeneration},json_extract(${yield* encodeProjectionJson(event.managerThreadId)},'$'),${event.occurredAt},NULL)`;
              } else {
                yield* guard(
                  custody.current?.managerGeneration === event.managerGeneration &&
                    custody.current.managerThreadId === event.managerThreadId,
                  "manager",
                );
                const changed =
                  yield* sql`UPDATE work_task_manager_bindings SET retired_at=${event.occurredAt} WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND generation=${event.managerGeneration} AND thread_id=json_extract(${yield* encodeProjectionJson(event.managerThreadId)},'$') AND retired_at IS NULL RETURNING 1`;
                yield* guard(changed.length === 1, "cas");
              }
              break;
            }
            case "task.allowance-configured":
            case "task.allowance-topped-up":
            case "task.allowance-debited": {
              const previous = (yield* allowanceAt(event.taskId, checkpoint!)).allowance;
              yield* guard(NodeUtil.isDeepStrictEqual(previous, event.previous), "allowance");
              const command = yield* one(
                event.taskId,
                "aggregate_kind='task' AND application_event_version=2 AND stream_id=json_extract(?,'$') AND command_id=json_extract(?,'$') AND event_type IN ('task.allowance-configured','task.allowance-topped-up','task.allowance-debited') AND sequence<=? ORDER BY sequence DESC LIMIT 1",
                [
                  yield* encodeProjectionJson(event.taskId),
                  yield* encodeProjectionJson(event.commandId),
                  checkpoint!,
                ],
                "idx_orch_events_command_id",
              );
              yield* guard(command === undefined, "command");
              if (event.type === "task.allowance-debited")
                yield* guard(
                  (yield* readAllowanceDebitSource({
                    taskId: event.taskId,
                    allowanceUnit: event.allowanceUnit,
                    checkpointSequence: checkpoint!,
                  })) === undefined,
                  "unit",
                );
              if (previous.type === "unconfigured") {
                yield* guard(
                  event.type === "task.allowance-configured" && previous.consumed === 0,
                  "allowance",
                );
                yield* sql`INSERT INTO work_task_allowances(task_id,configured_cap,consumed) VALUES(json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),${event.resulting.cap},${event.resulting.consumed})`;
              } else {
                const changed =
                  yield* sql`UPDATE work_task_allowances SET configured_cap=${event.resulting.cap},consumed=${event.resulting.consumed} WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND configured_cap=${previous.cap} AND consumed=${previous.consumed} RETURNING 1`;
                yield* guard(changed.length === 1, "cas");
              }
              break;
            }
            case "task.created":
              effective = event.definition;
              break;
            case "task.definition-updated":
            case "task.definition-accepted": {
              const draft = source.latestDefinition?.recorded.event;
              if (
                draft &&
                "definition" in draft &&
                draft.definition.revision === event.definitionRevision
              )
                yield* guard(
                  NodeUtil.isDeepStrictEqual(draft.definition, event.definition),
                  "definition",
                );
              const previous = yield* acceptedRow({
                taskId: event.taskId,
                revision: event.definitionRevision,
              });
              if (event.type === "task.definition-updated") {
                if (previous)
                  yield* guard(
                    NodeUtil.isDeepStrictEqual(previous.definition, event.definition),
                    "definition",
                  );
              } else {
                yield* guard(
                  !previous && !source.acceptance && context.commandId !== null,
                  "definition",
                );
                const latest = yield* acceptedRow({ taskId: event.taskId });
                if (latest?.actionPolicyRevision === event.actionPolicyRevision)
                  yield* guard(
                    NodeUtil.isDeepStrictEqual(
                      latest.definition.actionPolicy,
                      event.definition.actionPolicy,
                    ),
                    "policy",
                  );
              }
              effective = event.definition;
              break;
            }
            case "task.configuration-updated": {
              const previous = source.configuration?.recorded.event;
              if (
                previous?.type === "task.configuration-updated" &&
                previous.completionPolicyRevision === event.completionPolicyRevision
              )
                yield* guard(
                  NodeUtil.isDeepStrictEqual(previous.completionPolicy, event.completionPolicy),
                  "policy",
                );
              break;
            }
            case "task.execution-updated": {
              const known = source.manager?.recorded.event;
              const highest = source.highestManager?.recorded.event;
              if (event.manager) {
                if (known?.type === "task.execution-updated" && known.manager)
                  yield* guard(
                    known.manager.managerThreadId === event.manager.managerThreadId,
                    "manager",
                  );
                if (highest?.type === "task.execution-updated" && highest.manager)
                  yield* guard(
                    event.manager.managerGeneration >= highest.manager.managerGeneration,
                    "manager",
                  );
              }
              break;
            }
            case "task.thread-linked":
            case "task.thread-unlinked": {
              const links = yield* sql<{
                id: string;
              }>`SELECT hex(link_event_id) AS id FROM work_task_thread_links
          WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND thread_id=json_extract(${yield* encodeProjectionJson(event.threadId)},'$') AND unlink_event_id IS NULL`;
              activeLink = links[0] ? yield* attempt(() => decodeHex(links[0]!.id)) : undefined;
              yield* guard(
                event.type === "task.thread-linked" ? !activeLink : activeLink !== undefined,
                "link",
              );
              break;
            }
          }
          yield* guard(effective !== undefined, "definition");
          if (current) {
            const changed =
              yield* sql`UPDATE work_task_current_state SET last_applied_sequence=${context.sequence}
        WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND last_applied_sequence=${checkpoint}
        AND EXISTS(SELECT 1 FROM work_task_projections p WHERE p.task_id=work_task_current_state.task_id
          AND definition_revision=${current.definitionRevision} AND lifecycle_revision=${current.lifecycleRevision}
          AND completion_policy_revision=${current.completionPolicyRevision} AND action_policy_revision=${current.actionPolicyRevision}) RETURNING 1`;
            yield* guard(changed.length === 1, "cas");
            const updated =
              yield* sql`UPDATE work_task_projections SET definition_revision=${event.definitionRevision},lifecycle_revision=${event.lifecycleRevision},
        completion_policy_revision=${event.completionPolicyRevision},action_policy_revision=${event.actionPolicyRevision},definition_json=${yield* encodeProjectionJson(effective)},updated_at=${event.occurredAt}
        WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND definition_revision=${current.definitionRevision} AND lifecycle_revision=${current.lifecycleRevision}
        AND completion_policy_revision=${current.completionPolicyRevision} AND action_policy_revision=${current.actionPolicyRevision} RETURNING 1`;
            yield* guard(updated.length === 1, "cas");
          } else if (event.type === "task.created") {
            yield* sql`INSERT INTO work_task_projections VALUES(json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),json_extract(${yield* encodeProjectionJson(event.projectId ?? null)},'$'),
        ${event.definitionRevision},${event.lifecycleRevision},${event.completionPolicyRevision},${event.actionPolicyRevision},${yield* encodeProjectionJson(event.definition)},${event.occurredAt},${event.occurredAt})`;
            yield* sql`INSERT INTO work_task_current_state(task_id,last_applied_sequence) VALUES(json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),${context.sequence})`;
          }
          switch (event.type) {
            case "task.definition-accepted":
              yield* sql`INSERT INTO work_task_accepted_definitions VALUES(json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),${event.definitionRevision},${event.actionPolicyRevision},
          ${yield* encodeProjectionJson(event.definition)},${yield* encodeProjectionJson(event.attribution)},json_extract(${yield* encodeProjectionJson(event.id)},'$'),${event.occurredAt})`;
              break;
            case "task.configuration-updated":
              yield* sql`UPDATE work_task_current_state SET configuration_json=${yield* encodeProjectionJson({ allowanceOverride: event.allowanceOverride, completionPolicy: event.completionPolicy })} WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$')`;
              break;
            case "task.execution-updated":
              yield* sql`UPDATE work_task_current_state SET desired_mode=${event.desiredMode},manager_thread_id=json_extract(${yield* encodeProjectionJson(event.manager?.managerThreadId ?? null)},'$'),
          manager_generation=${event.manager?.managerGeneration ?? null} WHERE task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$')`;
              break;
            case "task.thread-linked":
              yield* sql`INSERT INTO work_task_thread_links(link_event_id,task_id,thread_id,linked_at,linked_attribution_json) VALUES(json_extract(${yield* encodeProjectionJson(event.id)},'$'),json_extract(${yield* encodeProjectionJson(event.taskId)},'$'),json_extract(${yield* encodeProjectionJson(event.threadId)},'$'),${event.occurredAt},${yield* encodeProjectionJson(event.attribution)})`;
              break;
            case "task.thread-unlinked": {
              const changed =
                yield* sql`UPDATE work_task_thread_links SET unlink_event_id=json_extract(${yield* encodeProjectionJson(event.id)},'$'),unlinked_at=${event.occurredAt},unlinked_attribution_json=${yield* encodeProjectionJson(event.attribution)}
          WHERE link_event_id=json_extract(${yield* encodeProjectionJson(activeLink)},'$') AND task_id=json_extract(${yield* encodeProjectionJson(event.taskId)},'$') AND unlink_event_id IS NULL RETURNING 1`;
              yield* guard(changed.length === 1, "cas");
              break;
            }
          }
          yield* publishReferenceCheckpoint(source.creation, original, checked);
          const advanced =
            yield* sql`UPDATE work_task_view_fence SET generation=${view.generation + 1},high_water_sequence=${Math.max(view.high, context.sequence)}
      WHERE singleton_id=1 AND generation=${view.generation} AND high_water_sequence=${view.high} RETURNING 1`;
          yield* guard(advanced.length === 1, "cas");
          return {
            applied: true,
            lastSequence: context.sequence,
            viewGeneration: view.generation + 1,
          };
        }),
      ),
  );
  const applyFoundation = Effect.fn("TaskStore.applyFoundation")((input: WorkTaskPersistedEvent) =>
    applyProjection(input, "foundation"),
  );
  const applyManagerAllowance = Effect.fn("TaskStore.applyManagerAllowance")(
    (input: WorkTaskPersistedEvent) => applyProjection(input, "managerAllowance"),
  );
  const rebuildFoundation = Effect.fn("TaskStore.rebuildFoundation")(
    (input: { taskId: string; throughSequence: number }) =>
      projectTransaction(
        Effect.gen(function* () {
          yield* guard((yield* readProjection(input.taskId)) === undefined, "legacy");
          let after: Anchor | undefined;
          do {
            const page = yield* readNativeHistory({ ...input, ...(after ? { after } : {}) });
            for (const value of page.records) yield* applyFoundation(value.recorded);
            after = page.next;
          } while (after);
          return yield* readFoundation(input.taskId);
        }),
      ),
  );

  return TaskStore.of({
    applyManagerAllowance,
    readManagerCustody,
    readManagerBinding,
    readAllowance,
    auditManagerReferences,
    readAllowanceDebitSource,
    applyFoundation,
    readFoundation,
    readAcceptedDefinition,
    readThreadLink,
    rebuildFoundation,
    readNativeEvent,
    readNativeSequence,
    readFoundationSource,
    readNativeHistory,
  });
});
export const layer = Layer.effect(TaskStore, make);
