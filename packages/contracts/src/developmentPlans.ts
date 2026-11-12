import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as Schema from "effect/Schema";

const NonEmpty = Schema.NonEmptyString;
const SourceCommon = {
  id: NonEmpty,
  environmentId: NonEmpty,
  projectId: NonEmpty,
  capturedAt: NonEmpty,
  attribution: NonEmpty,
  bytesBase64: Schema.String,
  byteLength: Schema.Number,
  sha256: NonEmpty,
};
const Source = Schema.Union([
  Schema.Struct({
    ...SourceCommon,
    kind: Schema.Literal("thread_message"),
    threadId: NonEmpty,
    messageId: NonEmpty,
  }),
  Schema.Struct({
    ...SourceCommon,
    kind: Schema.Literal("imported_document"),
    documentId: NonEmpty,
  }),
]);
const Statement = Schema.Struct({
  id: NonEmpty,
  sourceId: NonEmpty,
  startByte: Schema.Number,
  endByte: Schema.Number,
  text: NonEmpty,
});
const Criterion = Schema.Struct({ id: NonEmpty, text: NonEmpty });
const Requirement = Schema.Struct({
  id: NonEmpty,
  text: NonEmpty,
  sourceStatementIds: Schema.Array(NonEmpty),
  criteria: Schema.Array(Criterion),
});

export const DevelopmentPlanDocumentV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  planId: NonEmpty,
  revisionId: NonEmpty,
  environmentId: NonEmpty,
  projectId: NonEmpty,
  title: NonEmpty,
  brief: NonEmpty,
  linkedThreadIds: Schema.Array(NonEmpty),
  sources: Schema.Array(Source),
  sourceStatements: Schema.Array(Statement),
  requirements: Schema.Array(Requirement),
});
export type DevelopmentPlanDocumentV1 = typeof DevelopmentPlanDocumentV1.Type;

export class DevelopmentPlanDocumentError extends Schema.TaggedError<DevelopmentPlanDocumentError>()(
  "DevelopmentPlanDocumentError",
  {
    reason: Schema.Literals([
      "unsupported version",
      "unknown field",
      "ambiguous identity",
      "dangling reference",
      "invalid byte span",
      "source identity mismatch",
      "text differs from original selected bytes",
      "mixed discriminant shape",
      "malformed field",
    ]),
  },
) {}

type Reason = DevelopmentPlanDocumentError["reason"];
const fail = (reason: Reason) => new DevelopmentPlanDocumentError({ reason });
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const extra = (value: unknown, allowed: readonly string[]) =>
  record(value) && Object.keys(value).some((key) => !allowed.includes(key));
const each = (value: unknown, f: (item: unknown) => Reason | undefined): Reason | undefined => {
  if (!Array.isArray(value)) return undefined;
  for (const item of value) {
    const reason = f(item);
    if (reason) return reason;
  }
  return undefined;
};
const preflight = (input: unknown): Reason | undefined => {
  if (!record(input)) return undefined;
  if (input.schemaVersion !== 1) return "unsupported version";
  if (
    extra(input, [
      "schemaVersion",
      "planId",
      "revisionId",
      "environmentId",
      "projectId",
      "title",
      "brief",
      "linkedThreadIds",
      "sources",
      "sourceStatements",
      "requirements",
    ])
  )
    return "unknown field";
  return (
    each(input.sources, (source) => {
      if (!record(source)) return undefined;
      if (
        (source.kind === "thread_message" && "documentId" in source) ||
        (source.kind === "imported_document" && ("threadId" in source || "messageId" in source))
      )
        return "mixed discriminant shape";
      if (
        extra(source, [
          "id",
          "kind",
          "environmentId",
          "projectId",
          "capturedAt",
          "attribution",
          "bytesBase64",
          "byteLength",
          "sha256",
          ...(source.kind === "imported_document" ? ["documentId"] : ["threadId", "messageId"]),
        ])
      )
        return "unknown field";
      return undefined;
    }) ??
    each(input.sourceStatements, (statement) =>
      extra(statement, ["id", "sourceId", "startByte", "endByte", "text"])
        ? "unknown field"
        : undefined,
    ) ??
    each(input.requirements, (requirement) => {
      if (extra(requirement, ["id", "text", "sourceStatementIds", "criteria"]))
        return "unknown field";
      return record(requirement)
        ? each(requirement.criteria, (criterion) =>
            extra(criterion, ["id", "text"]) ? "unknown field" : undefined,
          )
        : undefined;
    })
  );
};
const duplicate = (values: readonly string[]) => new Set(values).size !== values.length;
const decodeShape = Schema.decodeUnknownEffect(DevelopmentPlanDocumentV1);
const validTimestamp = (value: string) => {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?Z$/.exec(value);
  if (!match) return false;
  const [, year, month, day, hour, minute, second] = match;
  const y = Number(year),
    m = Number(month),
    d = Number(day);
  const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    m >= 1 &&
    m <= 12 &&
    d >= 1 &&
    d <= days[m - 1]! &&
    Number(hour) <= 23 &&
    Number(minute) <= 59 &&
    Number(second) <= 59
  );
};

export const decodeDevelopmentPlanDocumentV1 = Effect.fn("DevelopmentPlanDocumentV1.decode")(
  function* (input: unknown) {
    const preflightError = preflight(input);
    if (preflightError) return yield* fail(preflightError);
    const document = yield* decodeShape(input, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => fail("malformed field")),
    );
    if (
      duplicate(document.linkedThreadIds) ||
      duplicate(document.sources.map((x) => x.id)) ||
      duplicate(document.sourceStatements.map((x) => x.id)) ||
      duplicate(document.requirements.map((x) => x.id)) ||
      duplicate(document.requirements.flatMap((x) => x.criteria.map((c) => c.id))) ||
      document.requirements.some((x) => duplicate(x.sourceStatementIds))
    )
      return yield* fail("ambiguous identity");
    const sourceBytes = new Map<string, Uint8Array>();
    for (const source of document.sources) {
      if (
        !validTimestamp(source.capturedAt) ||
        !Number.isSafeInteger(source.byteLength) ||
        source.byteLength < 0 ||
        !/^[0-9a-f]{64}$/.test(source.sha256)
      )
        return yield* fail("source identity mismatch");
      const bytes = yield* Effect.fromResult(Base64.decode(source.bytesBase64)).pipe(
        Effect.mapError(() => fail("source identity mismatch")),
      );
      if (Base64.encode(bytes) !== source.bytesBase64 || bytes.length !== source.byteLength)
        return yield* fail("source identity mismatch");
      const hash = yield* Effect.flatMap(Digest.Digest, (digest) =>
        digest.digest(Digest.sha256, bytes),
      ).pipe(Effect.mapError(() => fail("source identity mismatch")));
      if (Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("") !== source.sha256)
        return yield* fail("source identity mismatch");
      sourceBytes.set(source.id, bytes);
    }
    const statements = new Set(document.sourceStatements.map((x) => x.id));
    for (const requirement of document.requirements)
      if (requirement.sourceStatementIds.some((id) => !statements.has(id)))
        return yield* fail("dangling reference");
    for (const statement of document.sourceStatements) {
      const bytes = sourceBytes.get(statement.sourceId);
      if (!bytes) return yield* fail("dangling reference");
      if (
        !Number.isSafeInteger(statement.startByte) ||
        !Number.isSafeInteger(statement.endByte) ||
        statement.startByte < 0 ||
        statement.endByte < statement.startByte ||
        statement.endByte > bytes.length
      )
        return yield* fail("invalid byte span");
      const selected = yield* Effect.try({
        try: () =>
          new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
            bytes.subarray(statement.startByte, statement.endByte),
          ),
        catch: () => fail("text differs from original selected bytes"),
      });
      if (statement.text !== selected)
        return yield* fail("text differs from original selected bytes");
    }
    return document;
  },
);
