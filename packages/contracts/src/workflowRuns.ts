import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { EnvironmentId, ProjectId } from "./baseSchemas.ts";
import {
  decodeDevelopmentPlanDocumentV1,
  DevelopmentPlanDocumentV1,
  hasWorkflowDuplicates,
  isWorkflowTimestamp,
  isWorkflowUint,
  PlanningPassV1,
  PlanningSessionV1,
  RequirementId,
  Sha256Hex,
  WorkflowAccess,
  WorkflowProvider,
} from "./developmentPlans.ts";

const NonEmpty = Schema.NonEmptyString;
const CriterionId = NonEmpty.pipe(Schema.brand("CriterionId"));
const WorkflowItemId = NonEmpty.pipe(Schema.brand("WorkflowItemId"));
const WorkflowCheckId = NonEmpty.pipe(Schema.brand("WorkflowCheckId"));
const FileChange = Schema.Union([
  Schema.Struct({ kind: Schema.Literals(["create", "modify", "remove"]), path: NonEmpty }),
  Schema.Struct({ kind: Schema.Literal("rename"), fromPath: NonEmpty, toPath: NonEmpty }),
]);
const Command = Schema.Struct({
  id: WorkflowCheckId,
  argv: Schema.Array(Schema.String),
  cwd: NonEmpty,
  configHash: Sha256Hex,
});
const ResourceFields = {
  memoryBytes: Schema.Number,
  cpuQuotaMicrosPer100ms: Schema.Number,
  pidsMax: Schema.Number,
  perOperationWallSeconds: Schema.Number,
  writableBytes: Schema.Number,
  freeDiskReserveBytes: Schema.Number,
  environmentConcurrency: Schema.Number,
  brokerMaxConnections: Schema.Number,
  brokerMaxPendingRequests: Schema.Number,
  brokerBufferBytes: Schema.Number,
  brokerRequestWallSeconds: Schema.Number,
  stdoutStderrBytes: Schema.Number,
  structuredReviewBytes: Schema.Number,
  rawEvidenceBytes: Schema.Number,
};
export const WorkflowRunSpecV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  plan: PlanningPassV1.fields.binding,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: Schema.Struct({ root: NonEmpty, baseCommit: NonEmpty }),
  sourceDocument: Schema.Struct({
    document: DevelopmentPlanDocumentV1,
    sourcePacketHash: Sha256Hex,
  }),
  requirements: Schema.Array(
    Schema.Struct({
      id: RequirementId,
      text: NonEmpty,
      criteria: Schema.Array(Schema.Struct({ id: CriterionId, text: NonEmpty })),
    }),
  ),
  exclusions: Schema.Array(Schema.Struct({ requirementId: RequirementId, reason: NonEmpty })),
  items: Schema.Array(
    Schema.Struct({
      id: WorkflowItemId,
      goal: NonEmpty,
      files: Schema.Array(FileChange),
      criterionIds: Schema.Array(CriterionId),
      checkIds: Schema.Array(WorkflowCheckId),
      acceptance: NonEmpty,
    }),
  ),
  implementation: WorkflowProvider,
  reviewer: WorkflowProvider,
  checks: Schema.Array(Command),
  provisioning: Schema.Array(Command),
  registryHosts: Schema.Array(NonEmpty),
  permissionCeiling: Schema.Struct({
    implementation: Schema.Struct({
      kind: Schema.Literal("configured"),
      effectivePolicyHash: Sha256Hex,
    }),
    reviewer: Schema.Struct({
      kind: Schema.Literal("native_read_only"),
      effectivePolicyHash: Sha256Hex,
    }),
  }),
  resources: Schema.Struct(ResourceFields),
  limits: Schema.Struct({
    maxAttempts: Schema.Number,
    maxRepairRounds: Schema.Number,
    maxActiveWriters: Schema.Literal(1),
    deadline: NonEmpty,
  }),
  publication: Schema.Struct({
    kind: Schema.Literal("draft_pr"),
    baseRef: NonEmpty,
    requiredCi: Schema.Array(NonEmpty),
  }),
  access: WorkflowAccess,
});
export type WorkflowRunSpecV1 = typeof WorkflowRunSpecV1.Type;

export class WorkflowRunSpecError extends Schema.TaggedError<WorkflowRunSpecError>()(
  "WorkflowRunSpecError",
  {
    reason: Schema.Literals([
      "invalid limits or item",
      "invalid mapping",
      "invalid file scope",
      "invalid variant",
      "invalid record",
    ]),
  },
) {}
const fail = (reason: WorkflowRunSpecError["reason"]) => new WorkflowRunSpecError({ reason });
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const safePath = (path: string) =>
  !path.startsWith("/") &&
  !path.includes("\\") &&
  !/^[A-Za-z]:/.test(path) &&
  path.split("/").every((part) => part !== "" && part !== "." && part !== "..");
const hostname = (host: string) =>
  host.length <= 253 &&
  host
    .split(".")
    .every((label) => label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label));
const decodeShape = Schema.decodeUnknownEffect(WorkflowRunSpecV1);

export const decodeWorkflowRunSpecV1 = Effect.fn("WorkflowRunSpecV1.decode")(function* (
  input: unknown,
) {
  if (record(input)) {
    if (input.schemaVersion !== undefined && input.schemaVersion !== 1)
      return yield* fail("invalid variant");
    if (Array.isArray(input.items))
      for (const item of input.items) {
        if (!record(item)) continue;
        if (item.acceptance === "") return yield* fail("invalid limits or item");
        if (Array.isArray(item.files))
          for (const file of item.files) {
            if (!record(file)) continue;
            if (!["create", "modify", "remove", "rename"].includes(String(file.kind)))
              return yield* fail("invalid variant");
            if (
              (file.kind === "rename" && "path" in file) ||
              (file.kind !== "rename" && ("fromPath" in file || "toPath" in file))
            )
              return yield* fail("invalid variant");
            if (
              file.kind === "rename" &&
              (typeof file.fromPath !== "string" || typeof file.toPath !== "string")
            )
              return yield* fail("invalid file scope");
          }
      }
    const resources = input.resources;
    if (
      !record(resources) ||
      Object.keys(ResourceFields).some(
        (key) =>
          typeof resources[key] !== "number" ||
          !isWorkflowUint(resources[key] as number) ||
          resources[key] === 0,
      )
    )
      return yield* fail("invalid limits or item");
    if (
      !record(input.limits) ||
      typeof input.limits.maxAttempts !== "number" ||
      !isWorkflowUint(input.limits.maxAttempts) ||
      input.limits.maxAttempts === 0 ||
      typeof input.limits.maxRepairRounds !== "number" ||
      !isWorkflowUint(input.limits.maxRepairRounds) ||
      input.limits.maxActiveWriters !== 1 ||
      typeof input.limits.deadline !== "string" ||
      !isWorkflowTimestamp(input.limits.deadline)
    )
      return yield* fail("invalid limits or item");
  }
  const spec = yield* decodeShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => fail("invalid record")),
  );
  const document = yield* decodeDevelopmentPlanDocumentV1(spec.sourceDocument.document).pipe(
    Effect.mapError(() => fail("invalid record")),
  );
  if (
    spec.plan.planId !== document.planId ||
    spec.plan.revisionId !== document.revisionId ||
    spec.environmentId !== document.environmentId ||
    spec.projectId !== document.projectId ||
    spec.plan.sourcePacketHash !== spec.sourceDocument.sourcePacketHash ||
    !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(spec.repository.baseCommit)
  )
    return yield* fail("invalid record");
  if (
    !Object.values(spec.resources).every((n) => isWorkflowUint(n) && n > 0) ||
    !isWorkflowUint(spec.limits.maxAttempts) ||
    spec.limits.maxAttempts === 0 ||
    !isWorkflowUint(spec.limits.maxRepairRounds) ||
    !isWorkflowTimestamp(spec.limits.deadline) ||
    spec.items.some(
      (item) =>
        item.files.length === 0 || item.criterionIds.length === 0 || item.checkIds.length === 0,
    )
  )
    return yield* fail("invalid limits or item");
  if (hasWorkflowDuplicates(spec.items.map((item) => item.id)))
    return yield* fail("invalid mapping");
  const paths: string[] = [];
  for (const item of spec.items)
    for (const file of item.files) {
      const filePaths = file.kind === "rename" ? [file.fromPath, file.toPath] : [file.path];
      if (
        filePaths.some((path) => !safePath(path)) ||
        (file.kind === "rename" && file.fromPath === file.toPath)
      )
        return yield* fail("invalid file scope");
      paths.push(...filePaths);
    }
  if (hasWorkflowDuplicates(paths)) return yield* fail("invalid file scope");
  const allCommands = [...spec.checks, ...spec.provisioning];
  if (
    allCommands.some(
      (command) =>
        command.argv.length === 0 ||
        !command.argv[0] ||
        (command.cwd !== "." && !safePath(command.cwd)),
    )
  )
    return yield* fail("invalid record");
  if (
    hasWorkflowDuplicates(spec.registryHosts) ||
    spec.registryHosts.some((host) => !hostname(host)) ||
    hasWorkflowDuplicates(spec.publication.requiredCi) ||
    hasWorkflowDuplicates(
      spec.access.origins.map((o) => JSON.stringify([o.environmentId, o.projectId, o.threadId])),
    )
  )
    return yield* fail("invalid record");
  if (
    document.sources.some(
      (source) =>
        !spec.access.origins.some(
          (origin) =>
            origin.environmentId === source.environmentId &&
            origin.projectId === source.projectId &&
            (source.kind === "thread_message"
              ? origin.threadId === source.threadId
              : origin.threadId === undefined),
        ),
    )
  )
    return yield* fail("invalid record");
  const sourceRequirements = new Map(document.requirements.map((r) => [r.id, r]));
  const criteria = spec.requirements.flatMap((r) => r.criteria);
  const criterionIds = new Set(criteria.map((c) => c.id));
  const checkIds = new Set(spec.checks.map((c) => c.id));
  const selectedIds = new Set(spec.requirements.map((r) => r.id));
  const mapped = new Set(spec.items.flatMap((item) => item.criterionIds));
  if (
    hasWorkflowDuplicates(spec.requirements.map((r) => r.id)) ||
    hasWorkflowDuplicates(criteria.map((c) => c.id)) ||
    hasWorkflowDuplicates(spec.exclusions.map((e) => e.requirementId)) ||
    hasWorkflowDuplicates(spec.items.map((i) => i.id)) ||
    hasWorkflowDuplicates(allCommands.map((c) => c.id)) ||
    spec.requirements.some((r) => {
      const source = sourceRequirements.get(r.id);
      return (
        !source ||
        source.text !== r.text ||
        r.criteria.length === 0 ||
        r.criteria.some((c) => !source.criteria.some((s) => s.id === c.id && s.text === c.text)) ||
        r.criteria.length !== source.criteria.length
      );
    }) ||
    spec.exclusions.some(
      (e) => !sourceRequirements.has(e.requirementId) || selectedIds.has(e.requirementId),
    ) ||
    spec.requirements.length + spec.exclusions.length !== document.requirements.length ||
    spec.items.some(
      (i) =>
        hasWorkflowDuplicates(i.criterionIds) ||
        hasWorkflowDuplicates(i.checkIds) ||
        i.criterionIds.some((id) => !criterionIds.has(id)) ||
        i.checkIds.some((id) => !checkIds.has(id)),
    ) ||
    criteria.some((c) => !mapped.has(c.id))
  )
    return yield* fail("invalid mapping");
  return spec;
});

const WorkflowRunId = NonEmpty.pipe(Schema.brand("WorkflowRunId"));
const ReferenceId = PlanningPassV1.fields.inputManifest.fields.sources.value.fields.id;
export const WorkflowRunV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: WorkflowRunId,
  spec: Schema.Struct({
    ref: Schema.Struct({
      kind: Schema.Literal("configuration"),
      id: ReferenceId,
      sha256: Sha256Hex,
    }),
    plan: PlanningPassV1.fields.binding,
  }),
  mode: Schema.Literals(["ordinary", "qualification"]),
  origin: PlanningSessionV1.fields.origin,
  status: Schema.Literals([
    "active",
    "pausing",
    "paused",
    "stopping",
    "stopped",
    "completed",
    "needs_attention",
  ]),
  controlGeneration: Schema.Number,
  recordVersion: Schema.Number,
  allowance: Schema.Struct({
    maxAttempts: Schema.Number,
    maxRepairRounds: Schema.Number,
    maxActiveWriters: Schema.Literal(1),
    attemptsConsumed: Schema.Number,
    repairRoundsConsumed: Schema.Number,
    deadline: NonEmpty,
  }),
  amendmentSequence: Schema.Number,
  priorSpecHash: Schema.optionalKey(Sha256Hex),
  createdAt: NonEmpty,
  access: WorkflowAccess,
});
export type WorkflowRunV1 = typeof WorkflowRunV1.Type;

export class WorkflowRunError extends Schema.TaggedError<WorkflowRunError>()("WorkflowRunError", {
  reason: Schema.Literals(["invalid consumption", "invalid lineage", "invalid record"]),
}) {}
const runFail = (reason: WorkflowRunError["reason"]) => new WorkflowRunError({ reason });
const decodeRunShape = Schema.decodeUnknownEffect(WorkflowRunV1);
const timestampKey = (timestamp: string) => {
  const [seconds, fraction = ""] = timestamp.slice(0, -1).split(".");
  return `${seconds}.${fraction.replace(/0+$/, "")}`;
};

export const decodeWorkflowRunV1 = Effect.fn("WorkflowRunV1.decode")(function* (input: unknown) {
  const run = yield* decodeRunShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => runFail("invalid record")),
  );
  const allowance = run.allowance;
  if (
    !isWorkflowUint(run.controlGeneration) ||
    !isWorkflowUint(run.recordVersion) ||
    !isWorkflowUint(run.amendmentSequence) ||
    !isWorkflowUint(allowance.maxAttempts) ||
    allowance.maxAttempts === 0 ||
    !isWorkflowUint(allowance.maxRepairRounds) ||
    !isWorkflowUint(allowance.attemptsConsumed) ||
    !isWorkflowUint(allowance.repairRoundsConsumed) ||
    !isWorkflowTimestamp(run.createdAt) ||
    !isWorkflowTimestamp(allowance.deadline) ||
    timestampKey(allowance.deadline) < timestampKey(run.createdAt) ||
    hasWorkflowDuplicates(
      run.access.origins.map((o) => JSON.stringify([o.environmentId, o.projectId, o.threadId])),
    )
  )
    return yield* runFail("invalid record");
  if (
    allowance.attemptsConsumed > allowance.maxAttempts ||
    allowance.repairRoundsConsumed > allowance.maxRepairRounds ||
    allowance.repairRoundsConsumed > allowance.attemptsConsumed
  )
    return yield* runFail("invalid consumption");
  if (run.amendmentSequence > 0 !== (run.priorSpecHash !== undefined))
    return yield* runFail("invalid lineage");
  return run;
});
