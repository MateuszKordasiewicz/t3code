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

const WorkflowOperationId = PlanningPassV1.fields.operationId;
const WorkflowAttemptId = NonEmpty.pipe(Schema.brand("WorkflowAttemptId"));
const WorkspaceId = NonEmpty.pipe(Schema.brand("WorkspaceId"));
const RepositoryId = NonEmpty.pipe(Schema.brand("RepositoryId"));
const IncidentId = NonEmpty.pipe(Schema.brand("IncidentId"));
const BootId = NonEmpty.pipe(Schema.brand("BootId"));
const WorkerId = NonEmpty.pipe(Schema.brand("WorkerId"));
const WorkflowContext = Schema.Struct({
  specHash: Sha256Hex,
  policyHash: Sha256Hex,
  itemId: Schema.optionalKey(WorkflowItemId),
  attemptId: Schema.optionalKey(WorkflowAttemptId),
  candidateHash: Schema.optionalKey(Sha256Hex),
  configurationHash: Schema.optionalKey(Sha256Hex),
});
const operationKinds = [
  "planning_pass",
  "workspace_preparation",
  "workspace_release",
  "provisioning",
  "stage_launch",
  "check",
  "supervision",
  "publication",
  "ci_observation",
] as const;
const native = PlanningPassV1.fields.native.schema.fields;
const OperationParameters = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("planning_pass"),
    passId: PlanningPassV1.fields.id,
    binding: PlanningPassV1.fields.binding,
    role: PlanningPassV1.fields.role,
    provider: WorkflowProvider,
    limits: PlanningSessionV1.fields.limits,
    inputManifestHash: Sha256Hex,
  }),
  Schema.Struct({
    kind: Schema.Literal("workspace_preparation"),
    context: WorkflowContext,
    workspaceId: WorkspaceId,
    baseCommit: NonEmpty,
  }),
  Schema.Struct({
    kind: Schema.Literal("workspace_release"),
    context: WorkflowContext,
    workspaceId: WorkspaceId,
    retentionPolicyHash: Sha256Hex,
  }),
  Schema.Struct({
    kind: Schema.Literal("provisioning"),
    context: WorkflowContext,
    commandId: WorkflowCheckId,
    workspaceId: WorkspaceId,
  }),
  Schema.Struct({
    kind: Schema.Literal("stage_launch"),
    context: WorkflowContext,
    stage: Schema.Literals(["implementation", "code_review", "semantic_review"]),
    provider: WorkflowProvider,
    nativeThreadId: native.threadId,
    nativeRunId: native.runId,
  }),
  Schema.Struct({
    kind: Schema.Literal("check"),
    context: WorkflowContext,
    checkId: WorkflowCheckId,
    workspaceId: WorkspaceId,
  }),
  Schema.Struct({
    kind: Schema.Literal("supervision"),
    context: WorkflowContext,
    incidentId: IncidentId,
    provider: WorkflowProvider,
    nativeThreadId: native.threadId,
    nativeRunId: native.runId,
  }),
  Schema.Struct({
    kind: Schema.Literal("publication"),
    context: WorkflowContext,
    repositoryId: RepositoryId,
    baseRef: NonEmpty,
    headRef: NonEmpty,
    expectedRemoteHead: NonEmpty,
    candidateCommit: NonEmpty,
  }),
  Schema.Struct({
    kind: Schema.Literal("ci_observation"),
    context: WorkflowContext,
    repositoryId: RepositoryId,
    pullRequestId: NonEmpty,
    headCommit: NonEmpty,
    requiredChecks: Schema.Array(NonEmpty),
  }),
]);
const OperationReference = PlanningPassV1.fields.inputManifest.fields.sources.value;
export const WorkflowOperationV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: WorkflowOperationId,
  owner: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("workflow_run"), workflowRunId: WorkflowRunId }),
    Schema.Struct({
      kind: Schema.Literal("planning_session"),
      planningSessionId: PlanningSessionV1.fields.id,
    }),
  ]),
  kind: Schema.Literals(operationKinds),
  parameters: OperationParameters,
  fingerprint: Sha256Hex,
  origin: PlanningSessionV1.fields.origin,
  destination: Schema.Struct({ environmentId: EnvironmentId, projectId: ProjectId }),
  controlGeneration: Schema.Number,
  status: Schema.Literals(["pending", "claimed", "settled", "cancelled", "unknown"]),
  access: WorkflowAccess,
  claim: Schema.optionalKey(
    Schema.Struct({
      bootId: BootId,
      workerId: WorkerId,
      generation: Schema.Number,
      claimedAt: NonEmpty,
    }),
  ),
  outcome: Schema.optionalKey(
    Schema.Struct({
      kind: Schema.Literals(["succeeded", "failed", "cancelled", "unknown"]),
      summary: NonEmpty,
      evidence: Schema.Array(OperationReference),
    }),
  ),
  nextDueAt: Schema.optionalKey(NonEmpty),
});
export type WorkflowOperationV1 = typeof WorkflowOperationV1.Type;

export class WorkflowOperationError extends Schema.TaggedError<WorkflowOperationError>()(
  "WorkflowOperationError",
  { reason: Schema.Literals(["invalid owner union", "invalid journal state", "invalid record"]) },
) {}
const operationFail = (reason: WorkflowOperationError["reason"]) =>
  new WorkflowOperationError({ reason });
const decodeOperationShape = Schema.decodeUnknownEffect(WorkflowOperationV1);
export const decodeWorkflowOperationV1 = Effect.fn("WorkflowOperationV1.decode")(function* (
  input: unknown,
) {
  if (record(input) && record(input.owner)) {
    const owner = input.owner;
    if (
      (owner.kind === "planning_session" && "workflowRunId" in owner) ||
      (owner.kind === "workflow_run" && "planningSessionId" in owner)
    )
      return yield* operationFail("invalid owner union");
  }
  const operation = yield* decodeOperationShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => operationFail("invalid record")),
  );
  const { owner, parameters, status, claim, outcome } = operation;
  if (
    operation.kind !== parameters.kind ||
    (owner.kind === "planning_session") !== (parameters.kind === "planning_pass")
  )
    return yield* operationFail("invalid owner union");
  if (
    (status === "pending" && (claim !== undefined || outcome !== undefined)) ||
    (status === "claimed" && (claim === undefined || outcome !== undefined)) ||
    (status === "settled" && outcome?.kind !== "succeeded" && outcome?.kind !== "failed") ||
    (status === "cancelled" && outcome?.kind !== "cancelled") ||
    (status === "unknown" && outcome?.kind !== "unknown")
  )
    return yield* operationFail("invalid journal state");
  if (
    !isWorkflowUint(operation.controlGeneration) ||
    (claim !== undefined &&
      (!isWorkflowUint(claim.generation) || !isWorkflowTimestamp(claim.claimedAt))) ||
    (operation.nextDueAt !== undefined && !isWorkflowTimestamp(operation.nextDueAt)) ||
    hasWorkflowDuplicates(
      operation.access.origins.map((o) =>
        JSON.stringify([o.environmentId, o.projectId, o.threadId]),
      ),
    ) ||
    (outcome !== undefined &&
      hasWorkflowDuplicates(outcome.evidence.map((r) => JSON.stringify([r.kind, r.id]))))
  )
    return yield* operationFail("invalid record");
  if (parameters.kind === "planning_pass") {
    if (
      !isWorkflowUint(parameters.limits.maxPasses) ||
      parameters.limits.maxPasses === 0 ||
      !isWorkflowUint(parameters.limits.maxRepairRounds) ||
      !isWorkflowTimestamp(parameters.limits.deadline)
    )
      return yield* operationFail("invalid record");
  } else {
    const context = parameters.context;
    if (
      (parameters.kind === "stage_launch" || parameters.kind === "check") &&
      (context.itemId === undefined ||
        context.attemptId === undefined ||
        context.candidateHash === undefined ||
        context.configurationHash === undefined)
    )
      return yield* operationFail("invalid record");
    if (
      (parameters.kind === "publication" || parameters.kind === "ci_observation") &&
      (context.candidateHash === undefined || context.configurationHash === undefined)
    )
      return yield* operationFail("invalid record");
    if (parameters.kind === "ci_observation" && hasWorkflowDuplicates(parameters.requiredChecks))
      return yield* operationFail("invalid record");
  }
  return operation;
});

const WorkflowResultId = NonEmpty.pipe(Schema.brand("WorkflowResultId"));
const ReviewFindingId = NonEmpty.pipe(Schema.brand("ReviewFindingId"));
const RoleOutput = Schema.Struct({
  ref: OperationReference,
  byteLength: Schema.Number,
  availability: Schema.Literals(["available", "expired", "truncated"]),
  access: WorkflowAccess,
});
const RoleResult = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("check"),
    checkId: WorkflowCheckId,
    argv: Schema.Array(Schema.String),
    cwd: NonEmpty,
    exit: Schema.Union([
      Schema.Struct({ kind: Schema.Literal("exited"), code: Schema.Number }),
      Schema.Struct({ kind: Schema.Literal("cancelled") }),
      Schema.Struct({ kind: Schema.Literal("unknown") }),
    ]),
    truncated: Schema.Boolean,
    stdoutStderr: RoleOutput,
  }),
  Schema.Struct({
    kind: Schema.Literal("code_review"),
    verdict: Schema.Literals(["pass", "changes_requested", "inconclusive"]),
    findings: Schema.Array(
      Schema.Struct({
        id: ReviewFindingId,
        severity: Schema.Literals(["blocking", "nonblocking"]),
        description: NonEmpty,
        evidence: Schema.Array(OperationReference),
        requiredRepair: Schema.optionalKey(NonEmpty),
      }),
    ),
    output: RoleOutput,
  }),
  Schema.Struct({
    kind: Schema.Literal("semantic_review"),
    criteria: Schema.Array(
      Schema.Struct({
        requirementId: RequirementId,
        criterionId: CriterionId,
        verdict: Schema.Literals(["met", "unmet", "inconclusive"]),
        evidence: Schema.Array(OperationReference),
      }),
    ),
    output: RoleOutput,
  }),
  Schema.Struct({
    kind: Schema.Literal("rollup"),
    verdict: Schema.Literals(["accepted", "rejected", "inconclusive"]),
    resultRefs: Schema.Array(OperationReference),
    summary: NonEmpty,
  }),
]);
export const WorkflowRoleResultV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: WorkflowResultId,
  workflowRunId: WorkflowRunId,
  operationId: WorkflowOperationId,
  binding: Schema.Struct({
    specHash: Sha256Hex,
    candidateHash: Sha256Hex,
    configurationHash: Sha256Hex,
    policyHash: Sha256Hex,
    itemId: Schema.optionalKey(WorkflowItemId),
    attemptId: Schema.optionalKey(WorkflowAttemptId),
  }),
  result: RoleResult,
  access: WorkflowAccess,
});
export type WorkflowRoleResultV1 = typeof WorkflowRoleResultV1.Type;

export class WorkflowRoleResultError extends Schema.TaggedError<WorkflowRoleResultError>()(
  "WorkflowRoleResultError",
  {
    reason: Schema.Literals([
      "invalid evidence identity",
      "invalid verdict",
      "malformed result",
      "invalid record",
    ]),
  },
) {}
const roleFail = (reason: WorkflowRoleResultError["reason"]) =>
  new WorkflowRoleResultError({ reason });
const decodeRoleShape = Schema.decodeUnknownEffect(WorkflowRoleResultV1);
const originKey = (origin: (typeof WorkflowAccess.Type.origins)[number]) =>
  JSON.stringify([origin.environmentId, origin.projectId, origin.threadId]);
const duplicateRoleRefs = (refs: readonly (typeof OperationReference.Type)[]) =>
  hasWorkflowDuplicates(refs.map((ref) => JSON.stringify([ref.kind, ref.id])));

const conflictingRefIdentities = (
  refs: readonly { kind: string; id: string; sha256: string }[],
) => {
  const identities = new Map<string, string>();
  for (const ref of refs) {
    const key = JSON.stringify([ref.kind, ref.id]);
    if (identities.has(key) && identities.get(key) !== ref.sha256) return true;
    identities.set(key, ref.sha256);
  }
  return false;
};

export const decodeWorkflowRoleResultV1 = Effect.fn("WorkflowRoleResultV1.decode")(function* (
  input: unknown,
) {
  if (record(input) && record(input.binding) && !("candidateHash" in input.binding))
    return yield* roleFail("invalid evidence identity");
  if (
    record(input) &&
    record(input.result) &&
    input.result.kind === "check" &&
    record(input.result.exit)
  ) {
    const exit = input.result.exit;
    if (exit.kind !== "exited" && "code" in exit) return yield* roleFail("malformed result");
  }
  const value = yield* decodeRoleShape(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => roleFail("invalid record")),
  );
  const { binding, result, access } = value;
  if ((binding.itemId === undefined) !== (binding.attemptId === undefined))
    return yield* roleFail("malformed result");
  if (hasWorkflowDuplicates(access.origins.map(originKey)))
    return yield* roleFail("malformed result");
  const output =
    result.kind === "check"
      ? result.stdoutStderr
      : result.kind === "rollup"
        ? undefined
        : result.output;
  if (
    output &&
    (!isWorkflowUint(output.byteLength) ||
      hasWorkflowDuplicates(output.access.origins.map(originKey)) ||
      access.origins.some(
        (origin) =>
          !output.access.origins.some((candidate) => originKey(candidate) === originKey(origin)),
      ))
  )
    return yield* roleFail("malformed result");
  if (result.kind === "check") {
    if (
      result.argv.length === 0 ||
      !result.argv[0] ||
      (result.exit.kind === "exited" && !isWorkflowUint(result.exit.code))
    )
      return yield* roleFail("malformed result");
  } else if (result.kind === "code_review") {
    if (
      hasWorkflowDuplicates(result.findings.map((finding) => finding.id)) ||
      result.findings.some((finding) => duplicateRoleRefs(finding.evidence))
    )
      return yield* roleFail("malformed result");
    if (
      result.findings.some(
        (finding) =>
          finding.severity === "blocking" &&
          (finding.requiredRepair === undefined || result.verdict === "pass"),
      )
    )
      return yield* roleFail("invalid verdict");
  } else if (result.kind === "semantic_review") {
    if (
      hasWorkflowDuplicates(result.criteria.map((criterion) => criterion.criterionId)) ||
      result.criteria.some((criterion) => duplicateRoleRefs(criterion.evidence))
    )
      return yield* roleFail("malformed result");
    if (
      result.criteria.some(
        (criterion) => criterion.verdict === "met" && criterion.evidence.length === 0,
      )
    )
      return yield* roleFail("invalid verdict");
  } else if (duplicateRoleRefs(result.resultRefs)) return yield* roleFail("malformed result");
  const refs =
    result.kind === "check"
      ? [result.stdoutStderr.ref]
      : result.kind === "rollup"
        ? result.resultRefs
        : [
            result.output.ref,
            ...(result.kind === "code_review"
              ? result.findings.flatMap((finding) => finding.evidence)
              : result.criteria.flatMap((criterion) => criterion.evidence)),
          ];
  if (conflictingRefIdentities(refs)) return yield* roleFail("invalid evidence identity");
  return value;
});
