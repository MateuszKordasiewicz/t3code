import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as Schema from "effect/Schema";
import { EnvironmentId, MessageId, ProjectId, ThreadId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

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
export const hasWorkflowDuplicates = (values: readonly string[]) =>
  new Set(values).size !== values.length;
const decodeShape = Schema.decodeUnknownEffect(DevelopmentPlanDocumentV1);
export const isWorkflowTimestamp = (value: string) => {
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

const validateSourcePacket = Effect.fn("DevelopmentPlan.validateSourcePacket")(function* (
  sources: readonly (typeof Source.Type)[],
  sourceStatements: readonly (typeof Statement.Type)[],
  requirements: readonly { readonly sourceStatementIds: readonly string[] }[],
) {
  const sourceBytes = new Map<string, Uint8Array>();
  for (const source of sources) {
    if (
      !isWorkflowTimestamp(source.capturedAt) ||
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
  const statements = new Set(sourceStatements.map((x) => x.id));
  for (const requirement of requirements)
    if (requirement.sourceStatementIds.some((id) => !statements.has(id)))
      return yield* fail("dangling reference");
  for (const statement of sourceStatements) {
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
});

export const decodeDevelopmentPlanDocumentV1 = Effect.fn("DevelopmentPlanDocumentV1.decode")(
  function* (input: unknown) {
    const preflightError = preflight(input);
    if (preflightError) return yield* fail(preflightError);
    const document = yield* decodeShape(input, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => fail("malformed field")),
    );
    if (
      hasWorkflowDuplicates(document.linkedThreadIds) ||
      hasWorkflowDuplicates(document.sources.map((x) => x.id)) ||
      hasWorkflowDuplicates(document.sourceStatements.map((x) => x.id)) ||
      hasWorkflowDuplicates(document.requirements.map((x) => x.id)) ||
      hasWorkflowDuplicates(document.requirements.flatMap((x) => x.criteria.map((c) => c.id))) ||
      document.requirements.some((x) => hasWorkflowDuplicates(x.sourceStatementIds))
    )
      return yield* fail("ambiguous identity");
    yield* validateSourcePacket(document.sources, document.sourceStatements, document.requirements);
    return document;
  },
);

const SessionId = NonEmpty.pipe(Schema.brand("PlanningSessionId"));
const PlanId = NonEmpty.pipe(Schema.brand("PlanId"));
const ActorId = NonEmpty.pipe(Schema.brand("ActorId"));
const SessionOrigin = Schema.Struct({
  environmentId: EnvironmentId,
  projectId: ProjectId,
  threadId: Schema.optionalKey(ThreadId),
});
export const WorkflowProvider = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  model: NonEmpty,
});

export const PlanningSessionV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: SessionId,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  planId: PlanId,
  predecessorSessionId: Schema.optionalKey(SessionId),
  origin: Schema.Struct({ kind: Schema.Literals(["human", "agent", "system"]), id: ActorId }),
  request: NonEmpty,
  mode: Schema.Literal("ordinary"),
  allowedRoles: Schema.Array(Schema.Literals(["author", "review", "repair", "readback"])),
  allowedProviders: Schema.Array(WorkflowProvider),
  limits: Schema.Struct({
    maxPasses: Schema.Number,
    maxRepairRounds: Schema.Number,
    deadline: NonEmpty,
  }),
  consumed: Schema.Struct({ passes: Schema.Number, repairRounds: Schema.Number }),
  controlGeneration: Schema.Number,
  recordVersion: Schema.Number,
  status: Schema.Literals([
    "active",
    "pausing",
    "paused",
    "stopping",
    "stopped",
    "completed",
    "needs_attention",
  ]),
  createdAt: NonEmpty,
  access: Schema.Struct({ origins: Schema.Array(SessionOrigin) }),
});
export type PlanningSessionV1 = typeof PlanningSessionV1.Type;

export class PlanningSessionError extends Schema.TaggedError<PlanningSessionError>()(
  "PlanningSessionError",
  { reason: Schema.Literals(["invalid allowance", "invalid record"]) },
) {}

const decodeSessionShape = Schema.decodeUnknownEffect(PlanningSessionV1);
const sessionFail = (reason: PlanningSessionError["reason"]) =>
  new PlanningSessionError({ reason });
export const isWorkflowUint = (value: number) => Number.isSafeInteger(value) && value >= 0;
export const isWorkflowTimestampAfter = (a: string, b: string) => {
  const seconds = a.slice(0, 19).localeCompare(b.slice(0, 19));
  if (seconds !== 0) return seconds > 0;
  const fractionA = a.slice(19, -1).replace(/^\./, "").replace(/0+$/, "");
  const fractionB = b.slice(19, -1).replace(/^\./, "").replace(/0+$/, "");
  return fractionA > fractionB;
};

export const decodePlanningSessionV1 = Effect.fn("PlanningSessionV1.decode")(function* (
  input: unknown,
) {
  const session = yield* decodeSessionShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => sessionFail("invalid record")),
  );
  const { limits, consumed } = session;
  if (
    ![
      limits.maxPasses,
      limits.maxRepairRounds,
      consumed.passes,
      consumed.repairRounds,
      session.controlGeneration,
      session.recordVersion,
    ].every(isWorkflowUint) ||
    limits.maxPasses === 0 ||
    !isWorkflowTimestamp(limits.deadline) ||
    !isWorkflowTimestamp(session.createdAt) ||
    isWorkflowTimestampAfter(session.createdAt, limits.deadline) ||
    session.allowedRoles.length === 0 ||
    session.allowedProviders.length === 0 ||
    hasWorkflowDuplicates(session.allowedRoles) ||
    hasWorkflowDuplicates(
      session.allowedProviders.map((provider) =>
        JSON.stringify([provider.providerInstanceId, provider.model]),
      ),
    ) ||
    hasWorkflowDuplicates(
      session.access.origins.map((origin) =>
        JSON.stringify([origin.environmentId, origin.projectId, origin.threadId]),
      ),
    )
  )
    return yield* sessionFail("invalid record");
  if (
    consumed.passes > limits.maxPasses ||
    consumed.repairRounds > limits.maxRepairRounds ||
    consumed.repairRounds > consumed.passes
  )
    return yield* sessionFail("invalid allowance");
  return session;
});

const PlanningPassId = NonEmpty.pipe(Schema.brand("PlanningPassId"));
const RevisionId = NonEmpty.pipe(Schema.brand("RevisionId"));
const OperationId = NonEmpty.pipe(Schema.brand("OperationId"));
const NativeRunId = NonEmpty.pipe(Schema.brand("NativeRunId"));
const ReferenceId = NonEmpty.pipe(Schema.brand("ReferenceId"));
const InventoryId = NonEmpty.pipe(Schema.brand("InventoryId"));
export const RequirementId = NonEmpty.pipe(Schema.brand("RequirementId"));
const FindingId = NonEmpty.pipe(Schema.brand("FindingId"));
const ObjectId = NonEmpty.pipe(Schema.brand("ObjectId"));
export const Sha256Hex = NonEmpty.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const Reference = Schema.Struct({
  kind: Schema.Literals([
    "source",
    "plan_revision",
    "candidate",
    "configuration",
    "policy",
    "artifact",
    "report",
    "check",
    "review",
    "native_message",
  ]),
  id: ReferenceId,
  sha256: Sha256Hex,
});
export const WorkflowAccess = Schema.Struct({ origins: Schema.Array(SessionOrigin) });
const RevisionCriterion = Schema.Struct({
  ...Criterion.fields,
  explanation: Schema.optionalKey(NonEmpty),
  state: Schema.optionalKey(NonEmpty),
  sourceStatementIds: Schema.optionalKey(Schema.Array(NonEmpty)),
  access: Schema.optionalKey(WorkflowAccess),
});
const RevisionRequirement = Schema.Struct({
  id: RequirementId,
  text: NonEmpty,
  sourceStatementIds: Schema.Array(NonEmpty),
  criteria: Schema.Array(RevisionCriterion),
  explanation: Schema.optionalKey(NonEmpty),
  state: Schema.optionalKey(NonEmpty),
  access: Schema.optionalKey(WorkflowAccess),
});
const RevisionCommon = {
  id: NonEmpty,
  explanation: NonEmpty,
  sourceStatementIds: Schema.Array(NonEmpty),
  access: WorkflowAccess,
};
const Decision = Schema.Struct({
  ...RevisionCommon,
  text: NonEmpty,
  state: Schema.Literals(["open", "decided"]),
  alternatives: Schema.Array(Schema.Struct({ id: NonEmpty, text: NonEmpty })),
  selectedAlternativeId: Schema.optionalKey(NonEmpty),
  reason: Schema.optionalKey(NonEmpty),
});
const Question = Schema.Struct({
  ...RevisionCommon,
  text: NonEmpty,
  state: Schema.Literals(["open", "resolved"]),
  executionBlocking: Schema.Boolean,
  resolution: Schema.optionalKey(NonEmpty),
});
const Assumption = Schema.Struct({
  ...RevisionCommon,
  text: NonEmpty,
  state: Schema.Literals(["open", "confirmed", "rejected"]),
});
const Approach = Schema.Struct({
  ...RevisionCommon,
  text: NonEmpty,
  state: Schema.Literals(["draft", "active", "withdrawn"]),
});
const FileChange = Schema.Union([
  Schema.Struct({
    kind: Schema.Literals(["create", "modify", "remove"]),
    path: NonEmpty,
    intendedChange: NonEmpty,
  }),
  Schema.Struct({
    kind: Schema.Literal("rename"),
    fromPath: NonEmpty,
    toPath: NonEmpty,
    intendedChange: NonEmpty,
  }),
]);
const Verification = Schema.Struct({
  id: NonEmpty,
  description: NonEmpty,
  criterionIds: Schema.Array(NonEmpty),
});
const WorkItem = Schema.Struct({
  ...RevisionCommon,
  title: NonEmpty,
  state: Schema.Literals(["draft", "active", "withdrawn"]),
  requirementIds: Schema.Array(RequirementId),
  criterionIds: Schema.Array(NonEmpty),
  dependencyIds: Schema.Array(NonEmpty),
  fileChanges: Schema.Array(FileChange),
  verification: Schema.Array(Verification),
});
const ByteDocument = Schema.Struct({
  bytesBase64: Schema.String,
  byteLength: Schema.Number,
  sha256: NonEmpty,
});
const PlanningSourcePacket = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  sources: Schema.Array(Source),
  statements: Schema.Array(Statement),
  selectionLimitations: Schema.Array(Schema.String),
  access: WorkflowAccess,
});
export const DevelopmentPlanRevisionV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  planId: PlanId,
  sourcePacketHash: Sha256Hex,
  brief: NonEmpty,
  requirements: Schema.Array(RevisionRequirement),
  decisions: Schema.Array(Decision),
  questions: Schema.Array(Question),
  assumptions: Schema.Array(Assumption),
  approach: Schema.Array(Approach),
  workItems: Schema.Array(WorkItem),
  access: WorkflowAccess,
  briefDetails: Schema.optionalKey(
    Schema.Struct({ outcome: NonEmpty, reason: NonEmpty, boundaries: Schema.Array(NonEmpty) }),
  ),
  coverageReceipts: Schema.Array(
    Schema.Struct({
      id: NonEmpty,
      revisionHash: Sha256Hex,
      sourcePacketHash: Sha256Hex,
      structuralBlockers: Schema.Array(NonEmpty),
      createdAt: NonEmpty,
    }),
  ),
  unmappedMaterial: Schema.Array(Schema.Struct({ sourceId: NonEmpty, description: NonEmpty })),
  legacyDocument: Schema.optionalKey(ByteDocument),
});
export type DevelopmentPlanRevisionV1 = typeof DevelopmentPlanRevisionV1.Type;
export const RevisionEnvelope = Schema.Struct({
  planId: PlanId,
  revisionId: RevisionId,
  revisionHash: Sha256Hex,
  sourcePacketHash: Sha256Hex,
  revision: DevelopmentPlanRevisionV1,
  sourcePacket: PlanningSourcePacket,
  createdAt: NonEmpty,
});
export type RevisionEnvelope = typeof RevisionEnvelope.Type;
export class DevelopmentPlanRevisionError extends Schema.TaggedError<DevelopmentPlanRevisionError>()(
  "DevelopmentPlanRevisionError",
  {
    reason: Schema.Literals([
      "unsupported version",
      "unknown field",
      "malformed field",
      "ambiguous identity",
      "dangling reference",
      "invalid byte span",
      "source identity mismatch",
      "text differs from original selected bytes",
      "mixed discriminant shape",
      "invalid state",
      "dependency cycle",
      "binding mismatch",
      "self-referential receipt",
    ]),
  },
) {}
const revisionFail = (reason: DevelopmentPlanRevisionError["reason"]) =>
  new DevelopmentPlanRevisionError({ reason });
const decodeRevisionShape = Schema.decodeUnknownEffect(RevisionEnvelope);

type Shape = { readonly [key: string]: Shape | readonly [Shape] | null };
const originShape: Shape = { environmentId: null, projectId: null, threadId: null };
const accessShape: Shape = { origins: [originShape] };
const commonShape: Shape = {
  id: null,
  explanation: null,
  sourceStatementIds: null,
  state: null,
  access: accessShape,
};
const sourceShape: Shape = {
  id: null,
  kind: null,
  environmentId: null,
  projectId: null,
  capturedAt: null,
  attribution: null,
  bytesBase64: null,
  byteLength: null,
  sha256: null,
  threadId: null,
  messageId: null,
  documentId: null,
};
const revisionShape: Shape = {
  planId: null,
  revisionId: null,
  revisionHash: null,
  sourcePacketHash: null,
  createdAt: null,
  sourcePacket: {
    schemaVersion: null,
    sources: [sourceShape],
    statements: [{ id: null, sourceId: null, startByte: null, endByte: null, text: null }],
    selectionLimitations: null,
    access: accessShape,
  },
  revision: {
    schemaVersion: null,
    planId: null,
    sourcePacketHash: null,
    brief: null,
    access: accessShape,
    briefDetails: { outcome: null, reason: null, boundaries: null },
    legacyDocument: { bytesBase64: null, byteLength: null, sha256: null },
    requirements: [
      {
        id: null,
        text: null,
        sourceStatementIds: null,
        explanation: null,
        state: null,
        access: accessShape,
        criteria: [
          {
            id: null,
            text: null,
            explanation: null,
            state: null,
            sourceStatementIds: null,
            access: accessShape,
          },
        ],
      },
    ],
    decisions: [
      {
        ...commonShape,
        text: null,
        alternatives: [{ id: null, text: null }],
        selectedAlternativeId: null,
        reason: null,
      },
    ],
    questions: [{ ...commonShape, text: null, executionBlocking: null, resolution: null }],
    assumptions: [{ ...commonShape, text: null }],
    approach: [{ ...commonShape, text: null }],
    workItems: [
      {
        ...commonShape,
        title: null,
        requirementIds: null,
        criterionIds: null,
        dependencyIds: null,
        fileChanges: [
          { kind: null, path: null, fromPath: null, toPath: null, intendedChange: null },
        ],
        verification: [{ id: null, description: null, criterionIds: null }],
      },
    ],
    coverageReceipts: [
      {
        id: null,
        revisionHash: null,
        sourcePacketHash: null,
        structuralBlockers: null,
        createdAt: null,
      },
    ],
    unmappedMaterial: [{ sourceId: null, description: null }],
  },
};
const unknownRevisionField = (value: unknown, shape: Shape): boolean => {
  if (!record(value)) return false;
  return Object.entries(value).some(([key, item]) => {
    if (!(key in shape)) return true;
    const child = shape[key];
    if (!child) return false;
    if (Array.isArray(child))
      return Array.isArray(item) && item.some((entry) => unknownRevisionField(entry, child[0]!));
    return unknownRevisionField(item, child as Shape);
  });
};
const revisionPreflight = (input: unknown): DevelopmentPlanRevisionError["reason"] | undefined => {
  if (!record(input)) return undefined;
  if (
    (record(input.revision) &&
      input.revision.schemaVersion !== undefined &&
      input.revision.schemaVersion !== 1) ||
    (record(input.sourcePacket) &&
      input.sourcePacket.schemaVersion !== undefined &&
      input.sourcePacket.schemaVersion !== 1)
  )
    return "unsupported version";
  if (unknownRevisionField(input, revisionShape)) return "unknown field";
  if (record(input.sourcePacket)) {
    const issue = each(input.sourcePacket.sources, (s) =>
      record(s) &&
      ((s.kind === "thread_message" && "documentId" in s) ||
        (s.kind === "imported_document" && ("threadId" in s || "messageId" in s)))
        ? "mixed discriminant shape"
        : undefined,
    );
    if (issue) return issue;
  }
  if (record(input.revision)) {
    const issue = each(input.revision.workItems, (w) =>
      record(w)
        ? each(w.fileChanges, (f) =>
            record(f) &&
            ((f.kind === "rename" && "path" in f) ||
              (["create", "modify", "remove"].includes(String(f.kind)) &&
                ("fromPath" in f || "toPath" in f)))
              ? "mixed discriminant shape"
              : undefined,
          )
        : undefined,
    );
    if (issue) return issue;
  }
  return undefined;
};
const hasCycle = (items: readonly (typeof WorkItem.Type)[]) => {
  const byId = new Map(items.map((item) => [item.id, item]));
  const active = new Set<string>(),
    done = new Set<string>();
  const visit = (id: string): boolean => {
    if (active.has(id)) return true;
    if (done.has(id)) return false;
    active.add(id);
    if (byId.get(id)?.dependencyIds.some(visit)) return true;
    active.delete(id);
    done.add(id);
    return false;
  };
  return items.some((item) => visit(item.id));
};
export const decodeDevelopmentPlanRevisionV1 = Effect.fn("DevelopmentPlanRevisionV1.decode")(
  function* (input: unknown) {
    const preflightError = revisionPreflight(input);
    if (preflightError) return yield* revisionFail(preflightError);
    const envelope = yield* decodeRevisionShape(input, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => revisionFail("malformed field")),
    );
    const { revision: r, sourcePacket: p } = envelope;
    if (
      !isWorkflowTimestamp(envelope.createdAt) ||
      r.coverageReceipts.some((receipt) => !isWorkflowTimestamp(receipt.createdAt))
    )
      return yield* revisionFail("malformed field");
    if (envelope.planId !== r.planId || envelope.sourcePacketHash !== r.sourcePacketHash)
      return yield* revisionFail("binding mismatch");
    if (r.coverageReceipts.some((receipt) => receipt.revisionHash === envelope.revisionHash))
      return yield* revisionFail("self-referential receipt");
    const families = [
      p.sources,
      p.statements,
      r.requirements,
      r.decisions,
      r.questions,
      r.assumptions,
      r.approach,
      r.workItems,
      r.coverageReceipts,
      r.requirements.flatMap((x) => x.criteria),
      r.workItems.flatMap((x) => x.verification),
    ];
    const sourceRefs = [
      ...r.requirements,
      ...r.requirements
        .flatMap((x) => x.criteria)
        .filter((x) => x.sourceStatementIds !== undefined),
      ...r.decisions,
      ...r.questions,
      ...r.assumptions,
      ...r.approach,
      ...r.workItems,
    ];
    if (
      families.some((family) => hasWorkflowDuplicates(family.map((x) => x.id))) ||
      sourceRefs.some((x) => hasWorkflowDuplicates(x.sourceStatementIds ?? [])) ||
      r.decisions.some((d) => hasWorkflowDuplicates(d.alternatives.map((a) => a.id))) ||
      r.workItems.some((w) =>
        [
          w.requirementIds,
          w.criterionIds,
          w.dependencyIds,
          ...w.verification.map((v) => v.criterionIds),
        ].some(hasWorkflowDuplicates),
      )
    )
      return yield* revisionFail("ambiguous identity");
    if (
      r.decisions.some((d) =>
        d.state === "decided"
          ? !d.selectedAlternativeId ||
            !d.reason ||
            !d.alternatives.some((a) => a.id === d.selectedAlternativeId)
          : d.selectedAlternativeId !== undefined || d.reason !== undefined,
      ) ||
      r.questions.some((q) => (q.state === "resolved" ? !q.resolution : q.resolution !== undefined))
    )
      return yield* revisionFail("invalid state");
    const statements = new Set(p.statements.map((x) => x.id)),
      sources = new Set(p.sources.map((x) => x.id)),
      requirements = new Set(r.requirements.map((x) => x.id)),
      criteria = new Set(r.requirements.flatMap((x) => x.criteria.map((c) => c.id))),
      work = new Set(r.workItems.map((x) => x.id));
    if (
      sourceRefs.some((x) => x.sourceStatementIds?.some((id) => !statements.has(id))) ||
      r.unmappedMaterial.some((x) => !sources.has(x.sourceId)) ||
      r.workItems.some(
        (w) =>
          w.requirementIds.some((id) => !requirements.has(id)) ||
          w.criterionIds.some((id) => !criteria.has(id)) ||
          w.dependencyIds.some((id) => !work.has(id)) ||
          w.verification.some((v) => v.criterionIds.some((id) => !criteria.has(id))),
      )
    )
      return yield* revisionFail("dangling reference");
    if (hasCycle(r.workItems)) return yield* revisionFail("dependency cycle");
    yield* validateSourcePacket(p.sources, p.statements, []).pipe(
      Effect.mapError((error) => revisionFail(error.reason)),
    );
    if (r.legacyDocument)
      yield* validateSourcePacket(
        [
          {
            ...r.legacyDocument,
            id: "legacy",
            kind: "imported_document",
            environmentId: "legacy",
            projectId: "legacy",
            documentId: "legacy",
            capturedAt: "2000-01-01T00:00:00Z",
            attribution: "legacy",
          },
        ],
        [],
        [],
      ).pipe(Effect.mapError((error) => revisionFail(error.reason)));
    return envelope;
  },
);

const InputManifest = Schema.Struct({
  sources: Schema.Array(Reference),
  references: Schema.Array(Reference),
  additionalContext: Schema.Array(Reference),
  access: WorkflowAccess,
});
const Inventory = Schema.Struct({
  id: InventoryId,
  text: NonEmpty,
  sourceRefs: Schema.Array(Reference),
  requirementIds: Schema.Array(RequirementId),
  disposition: Schema.Literals(["mapped", "missing", "uncertain", "proposed_exclusion"]),
});
const Finding = Schema.Struct({
  id: FindingId,
  severity: Schema.Literals(["blocking", "nonblocking"]),
  description: NonEmpty,
  affectedObjectIds: Schema.Array(ObjectId),
  evidence: Schema.Array(Reference),
  proposedRemedy: NonEmpty,
});
const Disposition = Schema.Struct({
  reportId: PlanningPassId,
  findingId: FindingId,
  decision: Schema.Literals(["accept", "reject", "defer"]),
  rationale: NonEmpty,
  affectedObjectIds: Schema.Array(ObjectId),
  deferTrigger: Schema.optionalKey(NonEmpty),
});
const Report = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("author"),
    summary: NonEmpty,
    outputs: Schema.Array(Reference),
  }),
  Schema.Struct({
    kind: Schema.Literal("repair"),
    summary: NonEmpty,
    dispositions: Schema.Array(Disposition),
  }),
  Schema.Struct({
    kind: Schema.Literal("review"),
    verdict: Schema.Literals(["pass", "changes_requested", "inconclusive"]),
    assessment: Schema.Struct({
      sourcePacketHash: Sha256Hex,
      revisionHash: Sha256Hex,
      inventory: Schema.Array(Inventory),
    }),
    findings: Schema.Array(Finding),
  }),
  Schema.Struct({
    kind: Schema.Literal("readback"),
    buildDescription: NonEmpty,
    inferredIntent: NonEmpty,
    discrepancies: Schema.Array(NonEmpty),
    questions: Schema.Array(NonEmpty),
    actualInputs: InputManifest,
  }),
]);

export const PlanningPassV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: PlanningPassId,
  planningSessionId: SessionId,
  role: Schema.Literals(["author", "review", "repair", "readback"]),
  provider: WorkflowProvider,
  binding: Schema.Struct({
    planId: PlanId,
    revisionId: RevisionId,
    revisionSchemaVersion: Schema.Literal(1),
    revisionHash: Sha256Hex,
    sourcePacketHash: Sha256Hex,
  }),
  operationId: OperationId,
  inputManifest: InputManifest,
  status: Schema.Literals(["pending", "claimed", "settled", "cancelled", "unknown"]),
  native: Schema.optionalKey(
    Schema.Struct({
      threadId: ThreadId,
      runId: NativeRunId,
      terminalMessageId: Schema.optionalKey(MessageId),
    }),
  ),
  report: Schema.optionalKey(Report),
  access: WorkflowAccess,
});
export type PlanningPassV1 = typeof PlanningPassV1.Type;

export class PlanningPassError extends Schema.TaggedError<PlanningPassError>()(
  "PlanningPassError",
  {
    reason: Schema.Literals([
      "role or binding mismatch",
      "malformed report",
      "unbound report",
      "invalid record",
    ]),
  },
) {}
const passFail = (reason: PlanningPassError["reason"]) => new PlanningPassError({ reason });
const decodePassShape = Schema.decodeUnknownEffect(PlanningPassV1);
const duplicateReferences = (values: readonly (typeof Reference.Type)[]) =>
  hasWorkflowDuplicates(
    values.map((value) => JSON.stringify([value.kind, value.id, value.sha256])),
  );
const duplicateOrigins = (access: typeof WorkflowAccess.Type) =>
  hasWorkflowDuplicates(
    access.origins.map((origin) =>
      JSON.stringify([origin.environmentId, origin.projectId, origin.threadId]),
    ),
  );
const invalidManifest = (manifest: typeof InputManifest.Type) =>
  duplicateReferences(manifest.sources) ||
  duplicateReferences(manifest.references) ||
  duplicateReferences(manifest.additionalContext) ||
  duplicateOrigins(manifest.access);

export const decodePlanningPassV1 = Effect.fn("PlanningPassV1.decode")(function* (input: unknown) {
  const pass = yield* decodePassShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => passFail("invalid record")),
  );
  if (invalidManifest(pass.inputManifest) || duplicateOrigins(pass.access))
    return yield* passFail("invalid record");
  const report = pass.report;
  if (!report) return pass;
  if (
    pass.role !== report.kind ||
    (report.kind === "review" &&
      (report.assessment.sourcePacketHash !== pass.binding.sourcePacketHash ||
        report.assessment.revisionHash !== pass.binding.revisionHash))
  )
    return yield* passFail("role or binding mismatch");
  if (pass.status !== "settled" || !pass.native?.terminalMessageId)
    return yield* passFail("unbound report");
  if (
    (report.kind === "author" && duplicateReferences(report.outputs)) ||
    (report.kind === "repair" &&
      (hasWorkflowDuplicates(
        report.dispositions.map((d) => JSON.stringify([d.reportId, d.findingId])),
      ) ||
        report.dispositions.some(
          (d) =>
            hasWorkflowDuplicates(d.affectedObjectIds) ||
            (d.decision === "defer" ? !d.deferTrigger : d.deferTrigger !== undefined),
        ))) ||
    (report.kind === "readback" && invalidManifest(report.actualInputs)) ||
    (report.kind === "review" &&
      (hasWorkflowDuplicates(report.assessment.inventory.map((item) => item.id)) ||
        hasWorkflowDuplicates(report.findings.map((finding) => finding.id)) ||
        report.assessment.inventory.some(
          (item) =>
            item.sourceRefs.length === 0 ||
            duplicateReferences(item.sourceRefs) ||
            hasWorkflowDuplicates(item.requirementIds) ||
            (item.disposition === "mapped" && item.requirementIds.length === 0),
        ) ||
        report.findings.some(
          (finding) =>
            hasWorkflowDuplicates(finding.affectedObjectIds) ||
            duplicateReferences(finding.evidence),
        )))
  )
    return yield* passFail("malformed report");
  return pass;
});
