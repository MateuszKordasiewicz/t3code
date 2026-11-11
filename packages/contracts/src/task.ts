import * as Schema from "effect/Schema";

import {
  AuthSessionId,
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ProviderSessionId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./modelSelection.ts";

export const WorkTaskId = TrimmedNonEmptyString.pipe(Schema.brand("WorkTaskId"));
export type WorkTaskId = typeof WorkTaskId.Type;
export const WorkTaskCriterionId = TrimmedNonEmptyString.pipe(Schema.brand("WorkTaskCriterionId"));
export type WorkTaskCriterionId = typeof WorkTaskCriterionId.Type;
export const WorkTaskActionRuleId = TrimmedNonEmptyString.pipe(
  Schema.brand("WorkTaskActionRuleId"),
);
export type WorkTaskActionRuleId = typeof WorkTaskActionRuleId.Type;

const WORK_TASK_TITLE_MAX_LENGTH = 240;
const WORK_TASK_OBJECTIVE_MAX_LENGTH = 12_000;
const WORK_TASK_CRITERIA_MAX_COUNT = 64;
const WORK_TASK_CRITERION_TEXT_MAX_LENGTH = 2_000;
const WORK_TASK_DATA_MAX_LENGTH = 16_000;

const title = Schema.String.check(Schema.isMaxLength(WORK_TASK_TITLE_MAX_LENGTH));
const objective = Schema.String.check(Schema.isMaxLength(WORK_TASK_OBJECTIVE_MAX_LENGTH));
const criterionText = Schema.String.check(Schema.isMaxLength(WORK_TASK_CRITERION_TEXT_MAX_LENGTH));
const data = Schema.String.check(Schema.isMaxLength(WORK_TASK_DATA_MAX_LENGTH));
const nonBlank = Schema.makeFilter((value: string) => value.trim().length > 0, {
  expected: "a non-blank string",
});

export const WorkTaskDraftCriterion = Schema.Struct({
  id: WorkTaskCriterionId,
  condition: criterionText,
  verification: criterionText,
});
export type WorkTaskDraftCriterion = typeof WorkTaskDraftCriterion.Type;
export const WorkTaskAcceptedCriterion = Schema.Struct({
  id: WorkTaskCriterionId,
  condition: criterionText.check(nonBlank),
  verification: criterionText.check(nonBlank),
});
export type WorkTaskAcceptedCriterion = typeof WorkTaskAcceptedCriterion.Type;

const uniqueIds = Schema.makeFilter(
  (values: ReadonlyArray<{ readonly id: string }>) =>
    new Set(values.map((value) => value.id)).size === values.length,
);

// Rules match exact action and target values; they never grant provider or environment authority.
const exactScope = TrimmedNonEmptyString.check(
  Schema.isMaxLength(2_000),
  Schema.makeFilter((value) => !value.includes("*"), {
    expected: "an exact value without wildcards",
  }),
);
export const WorkTaskActionRule = Schema.Struct({
  id: WorkTaskActionRuleId,
  action: exactScope.check(Schema.isMaxLength(120)),
  target: exactScope,
  decision: Schema.Literals(["allow", "review-first", "deny"]),
});
export type WorkTaskActionRule = typeof WorkTaskActionRule.Type;
export const WorkTaskActionPolicy = Schema.Struct({
  version: Schema.Literal(1),
  rules: Schema.Array(WorkTaskActionRule).check(Schema.isMaxLength(32), uniqueIds),
}).annotate({ description: "Review-first requires a human decision and cannot grant authority." });
export type WorkTaskActionPolicy = typeof WorkTaskActionPolicy.Type;

export const WorkTaskDraftDefinition = Schema.Struct({
  revision: PositiveInt,
  title,
  objective,
  criteria: Schema.Array(WorkTaskDraftCriterion).check(
    Schema.isMaxLength(WORK_TASK_CRITERIA_MAX_COUNT),
    uniqueIds,
  ),
  data: Schema.optional(data),
  actionPolicy: WorkTaskActionPolicy,
});
export type WorkTaskDraftDefinition = typeof WorkTaskDraftDefinition.Type;
export const WorkTaskAcceptedDefinition = Schema.Struct({
  revision: PositiveInt,
  title: title.check(nonBlank),
  objective: objective.check(nonBlank),
  criteria: Schema.Array(WorkTaskAcceptedCriterion).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(WORK_TASK_CRITERIA_MAX_COUNT),
    uniqueIds,
  ),
  data: Schema.optional(data),
  actionPolicy: WorkTaskActionPolicy,
});
export type WorkTaskAcceptedDefinition = typeof WorkTaskAcceptedDefinition.Type;

export class WorkTaskError extends Schema.TaggedError<WorkTaskError>()("WorkTaskError", {
  code: Schema.Literals([
    "not_found",
    "invalid_definition",
    "revision_conflict",
    "invalid_state",
    "feature_disabled",
    "unsupported",
    "unavailable",
  ]),
  message: Schema.String,
  taskId: Schema.optional(WorkTaskId),
}) {}

// This describes server-derived context; authorization is performed by the service.
export const WorkTaskCallerContext = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("user"),
    sessionId: AuthSessionId,
    subject: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("agent"),
    threadId: ThreadId,
    providerSessionId: ProviderSessionId,
    runId: RunId,
  }),
  Schema.Struct({
    type: Schema.Literal("manager"),
    threadId: ThreadId,
    providerSessionId: ProviderSessionId,
    runId: RunId,
    generation: PositiveInt,
  }),
  Schema.Struct({ type: Schema.Literal("server") }),
]);
export type WorkTaskCallerContext = typeof WorkTaskCallerContext.Type;

const { revision: _revision, ...definitionFields } = WorkTaskDraftDefinition.fields;
const definitionContent = Schema.Struct(definitionFields);
const definitionGuards = {
  expectedDefinitionRevision: PositiveInt,
  expectedActionPolicyRevision: PositiveInt,
};
const taskCommand = { taskId: WorkTaskId, commandId: CommandId };
const committed = {
  taskId: WorkTaskId,
  definitionRevision: PositiveInt,
  lifecycleRevision: PositiveInt,
  actionPolicyRevision: PositiveInt,
  sequence: NonNegativeInt,
};

export const WorkTaskCreateDraftInput = Schema.Struct({
  ...taskCommand,
  projectId: Schema.optional(ProjectId),
  definition: definitionContent,
});
export type WorkTaskCreateDraftInput = typeof WorkTaskCreateDraftInput.Type;
export const WorkTaskCreateDraftResult = Schema.Struct(committed);
export type WorkTaskCreateDraftResult = typeof WorkTaskCreateDraftResult.Type;

export const WorkTaskProposeDefinitionInput = Schema.Struct({
  ...taskCommand,
  ...definitionGuards,
  definition: definitionContent,
});
export type WorkTaskProposeDefinitionInput = typeof WorkTaskProposeDefinitionInput.Type;
export const WorkTaskProposeDefinitionResult = Schema.Struct({
  ...committed,
  proposal: definitionContent,
});
export type WorkTaskProposeDefinitionResult = typeof WorkTaskProposeDefinitionResult.Type;

export const WorkTaskAdoptDefinitionInput = Schema.Struct({
  ...taskCommand,
  ...definitionGuards,
  expectedLifecycleRevision: PositiveInt,
  definition: definitionContent,
});
export type WorkTaskAdoptDefinitionInput = typeof WorkTaskAdoptDefinitionInput.Type;
export const WorkTaskAdoptDefinitionResult = Schema.Struct(committed);
export type WorkTaskAdoptDefinitionResult = typeof WorkTaskAdoptDefinitionResult.Type;

export const WorkTaskLinkThreadInput = Schema.Struct({
  ...taskCommand,
  expectedLifecycleRevision: PositiveInt,
  threadId: ThreadId,
});
export type WorkTaskLinkThreadInput = typeof WorkTaskLinkThreadInput.Type;
export const WorkTaskLinkThreadResult = Schema.Struct(committed);
export type WorkTaskLinkThreadResult = typeof WorkTaskLinkThreadResult.Type;

export const WorkTaskUnlinkThreadInput = Schema.Struct({
  ...taskCommand,
  expectedLifecycleRevision: PositiveInt,
  threadId: ThreadId,
});
export type WorkTaskUnlinkThreadInput = typeof WorkTaskUnlinkThreadInput.Type;
export const WorkTaskUnlinkThreadResult = Schema.Struct(committed);
export type WorkTaskUnlinkThreadResult = typeof WorkTaskUnlinkThreadResult.Type;

// Kept structurally aligned with thread launch without importing orchestration into task contracts.
export const WorkTaskWorkspaceSelection = Schema.Union([
  Schema.Struct({ type: Schema.Literal("root"), branch: Schema.optional(TrimmedNonEmptyString) }),
  Schema.Struct({
    type: Schema.Literal("existing_worktree"),
    worktreePath: TrimmedNonEmptyString,
    branch: Schema.optional(TrimmedNonEmptyString),
  }),
  Schema.Struct({
    type: Schema.Literal("worktree"),
    baseRef: TrimmedNonEmptyString,
    branch: Schema.optional(TrimmedNonEmptyString),
    startFromOrigin: Schema.optional(Schema.Boolean),
  }),
]);
export type WorkTaskWorkspaceSelection = typeof WorkTaskWorkspaceSelection.Type;
export const WorkTaskLaunchSelection = Schema.Struct({
  projectId: ProjectId,
  modelSelection: ModelSelection,
  workspace: WorkTaskWorkspaceSelection,
});
export type WorkTaskLaunchSelection = typeof WorkTaskLaunchSelection.Type;

export const WorkTaskCompletionPolicy = Schema.Union([
  Schema.Struct({ type: Schema.Literal("manual") }),
  Schema.Struct({ type: Schema.Literal("automatic"), validator: WorkTaskLaunchSelection }),
]);
export type WorkTaskCompletionPolicy = typeof WorkTaskCompletionPolicy.Type;
export const WorkTaskCompletionPolicyOverride = Schema.Union([
  Schema.Struct({ type: Schema.Literal("inherit") }),
  Schema.Struct({ type: Schema.Literal("configured"), policy: WorkTaskCompletionPolicy }),
]);
export type WorkTaskCompletionPolicyOverride = typeof WorkTaskCompletionPolicyOverride.Type;

const lifecycleGuards = {
  expectedLifecycleRevision: PositiveInt,
  expectedManagerGeneration: Schema.NullOr(PositiveInt),
};
const executionGuards = {
  ...definitionGuards,
  ...lifecycleGuards,
  expectedCompletionPolicyRevision: PositiveInt,
};
const lifecycleResult = { ...committed, completionPolicyRevision: PositiveInt };

export const WorkTaskConfigureInput = Schema.Struct({
  ...taskCommand,
  ...lifecycleGuards,
  expectedCompletionPolicyRevision: PositiveInt,
  allowanceOverride: Schema.NullOr(PositiveInt),
  completionPolicy: WorkTaskCompletionPolicyOverride,
});
export type WorkTaskConfigureInput = typeof WorkTaskConfigureInput.Type;
export const WorkTaskConfigureResult = Schema.Struct(lifecycleResult);
export type WorkTaskConfigureResult = typeof WorkTaskConfigureResult.Type;
export const WorkTaskTopUpAllowanceInput = Schema.Struct({
  ...taskCommand,
  ...lifecycleGuards,
  newAllowanceCap: PositiveInt,
});
export type WorkTaskTopUpAllowanceInput = typeof WorkTaskTopUpAllowanceInput.Type;
export const WorkTaskTopUpAllowanceResult = Schema.Struct(lifecycleResult);
export type WorkTaskTopUpAllowanceResult = typeof WorkTaskTopUpAllowanceResult.Type;

export const WorkTaskStartInput = Schema.Struct({
  ...taskCommand,
  ...executionGuards,
  manager: WorkTaskLaunchSelection,
});
export type WorkTaskStartInput = typeof WorkTaskStartInput.Type;
export const WorkTaskStartResult = Schema.Struct(lifecycleResult);
export type WorkTaskStartResult = typeof WorkTaskStartResult.Type;
export const WorkTaskReplaceManagerInput = Schema.Struct({
  ...taskCommand,
  ...executionGuards,
  manager: WorkTaskLaunchSelection,
});
export type WorkTaskReplaceManagerInput = typeof WorkTaskReplaceManagerInput.Type;
export const WorkTaskReplaceManagerResult = Schema.Struct(lifecycleResult);
export type WorkTaskReplaceManagerResult = typeof WorkTaskReplaceManagerResult.Type;

export const WorkTaskPauseInput = Schema.Struct({ ...taskCommand, ...lifecycleGuards });
export type WorkTaskPauseInput = typeof WorkTaskPauseInput.Type;
export const WorkTaskPauseResult = Schema.Struct(lifecycleResult);
export type WorkTaskPauseResult = typeof WorkTaskPauseResult.Type;
export const WorkTaskResumeInput = Schema.Struct({ ...taskCommand, ...executionGuards });
export type WorkTaskResumeInput = typeof WorkTaskResumeInput.Type;
export const WorkTaskResumeResult = Schema.Struct(lifecycleResult);
export type WorkTaskResumeResult = typeof WorkTaskResumeResult.Type;
export const WorkTaskReopenInput = Schema.Struct({ ...taskCommand, ...executionGuards });
export type WorkTaskReopenInput = typeof WorkTaskReopenInput.Type;
export const WorkTaskReopenResult = Schema.Struct(lifecycleResult);
export type WorkTaskReopenResult = typeof WorkTaskReopenResult.Type;
export const WorkTaskArchiveInput = Schema.Struct({ ...taskCommand, ...lifecycleGuards });
export type WorkTaskArchiveInput = typeof WorkTaskArchiveInput.Type;
export const WorkTaskArchiveResult = Schema.Struct(lifecycleResult);
export type WorkTaskArchiveResult = typeof WorkTaskArchiveResult.Type;
export const WorkTaskUnarchiveInput = Schema.Struct({ ...taskCommand, ...lifecycleGuards });
export type WorkTaskUnarchiveInput = typeof WorkTaskUnarchiveInput.Type;
export const WorkTaskUnarchiveResult = Schema.Struct(lifecycleResult);
export type WorkTaskUnarchiveResult = typeof WorkTaskUnarchiveResult.Type;
export const WorkTaskCompleteManuallyInput = Schema.Struct({ ...taskCommand, ...executionGuards });
export type WorkTaskCompleteManuallyInput = typeof WorkTaskCompleteManuallyInput.Type;
export const WorkTaskCompleteManuallyResult = Schema.Struct(lifecycleResult);
export type WorkTaskCompleteManuallyResult = typeof WorkTaskCompleteManuallyResult.Type;

// Tokens are opaque and must be bound to the environment and query by the service.
export const WorkTaskPageToken = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000)).pipe(
  Schema.brand("WorkTaskPageToken"),
);
export type WorkTaskPageToken = typeof WorkTaskPageToken.Type;
export const WorkTaskReplayCursor = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000)).pipe(
  Schema.brand("WorkTaskReplayCursor"),
);
export type WorkTaskReplayCursor = typeof WorkTaskReplayCursor.Type;
const pageLimit = PositiveInt.check(Schema.isLessThanOrEqualTo(100));
const pageInput = {
  limit: Schema.optional(pageLimit),
  pageToken: Schema.optional(WorkTaskPageToken),
};
const snapshot = { sequence: NonNegativeInt, nextPageToken: Schema.optional(WorkTaskPageToken) };
const shortText = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000));

export const WorkTaskSummary = Schema.Struct({
  taskId: WorkTaskId,
  projectId: Schema.optional(ProjectId),
  title,
  definitionRevision: PositiveInt,
  lifecycleRevision: PositiveInt,
  desiredMode: Schema.Literals(["not_started", "active", "paused", "closed"]),
  archived: Schema.Boolean,
  execution: Schema.Struct({
    state: Schema.Literals(["not_started", "idle", "queued", "running", "waiting"]),
    managerThreadId: Schema.optional(ThreadId),
    managerGeneration: Schema.optional(PositiveInt),
  }),
  attention: Schema.Union([
    Schema.Struct({ state: Schema.Literal("none") }),
    Schema.Struct({ state: Schema.Literal("needed"), reason: shortText }),
  ]),
  completion: Schema.Literals(["incomplete", "ready_for_review", "validating", "done"]),
});
export type WorkTaskSummary = typeof WorkTaskSummary.Type;

export const WorkTaskListInput = Schema.Struct({
  ...pageInput,
  projectId: Schema.optional(ProjectId),
});
export type WorkTaskListInput = typeof WorkTaskListInput.Type;
export const WorkTaskListResult = Schema.Struct({
  ...snapshot,
  tasks: Schema.Array(WorkTaskSummary).check(Schema.isMaxLength(100)),
});
export type WorkTaskListResult = typeof WorkTaskListResult.Type;
export const WorkTaskDetailInput = Schema.Struct({ taskId: WorkTaskId });
export type WorkTaskDetailInput = typeof WorkTaskDetailInput.Type;
export const WorkTaskDetailResult = Schema.Struct({
  sequence: NonNegativeInt,
  summary: WorkTaskSummary,
  definition: WorkTaskDraftDefinition,
  actionPolicyRevision: PositiveInt,
  completionPolicyRevision: PositiveInt,
  allowanceOverride: Schema.NullOr(PositiveInt),
  completionPolicy: WorkTaskCompletionPolicyOverride,
  resolvedCompletionPolicy: Schema.optional(WorkTaskCompletionPolicy),
  resolvedAllowance: Schema.optional(PositiveInt),
  consumedAllowance: NonNegativeInt,
});
export type WorkTaskDetailResult = typeof WorkTaskDetailResult.Type;

export const WorkTaskThreadLink = Schema.Struct({
  threadId: ThreadId,
  role: Schema.Literals(["manager", "contributor"]),
});
export type WorkTaskThreadLink = typeof WorkTaskThreadLink.Type;
export const WorkTaskThreadLinksInput = Schema.Struct({ taskId: WorkTaskId, ...pageInput });
export type WorkTaskThreadLinksInput = typeof WorkTaskThreadLinksInput.Type;
export const WorkTaskThreadLinksResult = Schema.Struct({
  ...snapshot,
  links: Schema.Array(WorkTaskThreadLink).check(Schema.isMaxLength(100)),
});
export type WorkTaskThreadLinksResult = typeof WorkTaskThreadLinksResult.Type;

const referenceThreadId = ThreadId.check(Schema.isMaxLength(2_000));
const referenceRunId = RunId.check(Schema.isMaxLength(2_000));
export const WorkTaskResultReference = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("run"),
    threadId: referenceThreadId,
    runId: referenceRunId,
  }),
  Schema.Struct({ type: Schema.Literal("artifact"), uri: shortText, hash: shortText }),
  Schema.Struct({
    type: Schema.Literal("source_revision"),
    repository: shortText,
    revision: shortText,
  }),
]);
export type WorkTaskResultReference = typeof WorkTaskResultReference.Type;
export const WorkTaskContribution = Schema.Struct({
  id: shortText,
  threadId: ThreadId,
  sequence: NonNegativeInt,
  summary: shortText,
  results: Schema.Array(WorkTaskResultReference).check(Schema.isMaxLength(32)),
});
export type WorkTaskContribution = typeof WorkTaskContribution.Type;
export const WorkTaskHistoryInput = Schema.Struct({ taskId: WorkTaskId, ...pageInput });
export type WorkTaskHistoryInput = typeof WorkTaskHistoryInput.Type;
export const WorkTaskHistoryResult = Schema.Struct({
  ...snapshot,
  contributions: Schema.Array(WorkTaskContribution).check(Schema.isMaxLength(100)),
});
export type WorkTaskHistoryResult = typeof WorkTaskHistoryResult.Type;

// Callers must be authorized for task metadata before reverse memberships are returned.
export const WorkTaskMembershipsInput = Schema.Struct({ threadId: ThreadId, ...pageInput });
export type WorkTaskMembershipsInput = typeof WorkTaskMembershipsInput.Type;
export const WorkTaskMembershipsResult = Schema.Struct({
  ...snapshot,
  tasks: Schema.Array(WorkTaskSummary).check(Schema.isMaxLength(100)),
});
export type WorkTaskMembershipsResult = typeof WorkTaskMembershipsResult.Type;

export const WorkTaskInvalidation = Schema.Struct({
  taskId: WorkTaskId,
  revision: PositiveInt,
  sequence: NonNegativeInt,
  change: Schema.Literals([
    "definition",
    "lifecycle",
    "execution",
    "membership",
    "contribution",
    "completion",
    "deleted",
  ]),
});
export type WorkTaskInvalidation = typeof WorkTaskInvalidation.Type;
export const WorkTaskSubscribeInput = Schema.Struct({
  cursor: Schema.optional(WorkTaskReplayCursor),
});
export type WorkTaskSubscribeInput = typeof WorkTaskSubscribeInput.Type;
export const WorkTaskSubscriptionEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("invalidation"),
    invalidation: WorkTaskInvalidation,
    cursor: WorkTaskReplayCursor,
  }),
  Schema.Struct({
    type: Schema.Literal("resync-required"),
    reason: Schema.Literals(["cursor-expired", "buffer-overflow", "replay-unavailable"]),
    sequence: NonNegativeInt,
  }),
]);
export type WorkTaskSubscriptionEvent = typeof WorkTaskSubscriptionEvent.Type;

const workTaskReferenceBatch = Schema.Array(WorkTaskResultReference).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(32),
);
const managerGuards = {
  expectedDefinitionRevision: PositiveInt,
  expectedManagerGeneration: PositiveInt,
};

export const WorkTaskWaitTarget = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("run"),
    threadId: referenceThreadId,
    runId: referenceRunId,
  }),
  Schema.Struct({ type: Schema.Literal("source"), sourceId: shortText }),
  Schema.Struct({ type: Schema.Literal("result"), result: WorkTaskResultReference }),
]);
export type WorkTaskWaitTarget = typeof WorkTaskWaitTarget.Type;
export const WorkTaskRegisterWaitInput = Schema.Struct({
  ...taskCommand,
  ...managerGuards,
  waitId: shortText,
  target: WorkTaskWaitTarget,
});
export type WorkTaskRegisterWaitInput = typeof WorkTaskRegisterWaitInput.Type;
export const WorkTaskRegisterWaitResult = Schema.Struct({ ...committed, waitId: shortText });
export type WorkTaskRegisterWaitResult = typeof WorkTaskRegisterWaitResult.Type;
export const WorkTaskClearWaitInput = Schema.Struct({
  ...taskCommand,
  ...managerGuards,
  waitId: shortText,
});
export type WorkTaskClearWaitInput = typeof WorkTaskClearWaitInput.Type;
export const WorkTaskClearWaitResult = Schema.Struct({ ...committed, waitId: shortText });
export type WorkTaskClearWaitResult = typeof WorkTaskClearWaitResult.Type;

export const WorkTaskSubmitResultsInput = Schema.Struct({
  ...taskCommand,
  expectedDefinitionRevision: PositiveInt,
  sourceId: shortText,
  summary: shortText,
  results: workTaskReferenceBatch,
});
export type WorkTaskSubmitResultsInput = typeof WorkTaskSubmitResultsInput.Type;
export const WorkTaskSubmitResultsResult = Schema.Struct({
  ...committed,
  sourceId: shortText,
  attribution: WorkTaskCallerContext,
});
export type WorkTaskSubmitResultsResult = typeof WorkTaskSubmitResultsResult.Type;

export const WorkTaskReportBlockerInput = Schema.Struct({
  ...taskCommand,
  ...managerGuards,
  reason: shortText,
});
export type WorkTaskReportBlockerInput = typeof WorkTaskReportBlockerInput.Type;
export const WorkTaskReportBlockerResult = Schema.Struct(committed);
export type WorkTaskReportBlockerResult = typeof WorkTaskReportBlockerResult.Type;

export const WorkTaskSubmitCompletionClaimInput = Schema.Struct({
  ...taskCommand,
  ...managerGuards,
  expectedCompletionPolicyRevision: PositiveInt,
  summary: shortText,
  evidence: workTaskReferenceBatch,
});
export type WorkTaskSubmitCompletionClaimInput = typeof WorkTaskSubmitCompletionClaimInput.Type;
// Claim and evidence identities are assigned by the service for subsequent guarded review.
export const WorkTaskSubmitCompletionClaimResult = Schema.Struct({
  ...committed,
  managerGeneration: PositiveInt,
  completionPolicyRevision: PositiveInt,
  claimId: shortText,
  evidenceId: shortText,
});
export type WorkTaskSubmitCompletionClaimResult = typeof WorkTaskSubmitCompletionClaimResult.Type;

const reviewTaskId = WorkTaskId.check(Schema.isMaxLength(2_000));
const reviewCommandId = CommandId.check(Schema.isMaxLength(2_000));
const reviewId = shortText;
const actionFingerprint = shortText;
const reviewArtifact = Schema.Struct({ uri: shortText, hash: shortText });
const utf8 = new TextEncoder();

export const WorkTaskActionPreview = Schema.Struct({
  content: Schema.String.check(Schema.isMaxLength(32 * 1024), nonBlank),
  artifacts: Schema.Array(reviewArtifact).check(Schema.isMaxLength(16)),
}).check(
  Schema.makeFilter((preview) => utf8.encode(JSON.stringify(preview)).byteLength <= 32 * 1024, {
    expected: "a complete preview of at most 32768 UTF-8 bytes",
  }),
);
export type WorkTaskActionPreview = typeof WorkTaskActionPreview.Type;

export const WorkTaskActionReviewDecision = Schema.Literals([
  "approve",
  "reject",
  "request_changes",
]);
export type WorkTaskActionReviewDecision = typeof WorkTaskActionReviewDecision.Type;

const humanDecision = {
  subject: shortText,
  sessionId: AuthSessionId.check(Schema.isMaxLength(2_000)),
  feedback: Schema.optional(shortText),
  resolvedSequence: NonNegativeInt,
  resolvedAt: shortText,
};
export const WorkTaskActionReviewState = Schema.Union([
  Schema.Struct({ type: Schema.Literal("pending") }),
  Schema.Struct({ type: Schema.Literal("approved"), ...humanDecision }),
  Schema.Struct({ type: Schema.Literal("rejected"), ...humanDecision }),
  Schema.Struct({ type: Schema.Literal("changes_requested"), ...humanDecision }),
  Schema.Struct({
    type: Schema.Literal("invalidated"),
    reason: shortText,
    resolvedSequence: NonNegativeInt,
    resolvedAt: shortText,
  }),
  Schema.Struct({
    type: Schema.Literal("cancelled"),
    reason: shortText,
    resolvedSequence: NonNegativeInt,
    resolvedAt: shortText,
  }),
]);
export type WorkTaskActionReviewState = typeof WorkTaskActionReviewState.Type;

const reviewProposal = {
  action: exactScope.check(Schema.isMaxLength(120)),
  label: shortText,
  target: exactScope,
  preview: WorkTaskActionPreview,
  fingerprint: actionFingerprint,
};
export const WorkTaskActionReview = Schema.Struct({
  taskId: reviewTaskId,
  reviewId,
  revision: PositiveInt,
  managerThreadId: referenceThreadId,
  managerGeneration: PositiveInt,
  providerSessionId: ProviderSessionId.check(Schema.isMaxLength(2_000)),
  runId: referenceRunId,
  ruleId: WorkTaskActionRuleId.check(Schema.isMaxLength(2_000)),
  definitionRevision: PositiveInt,
  actionPolicyRevision: PositiveInt,
  ...reviewProposal,
  state: WorkTaskActionReviewState,
  createdSequence: NonNegativeInt,
  createdAt: shortText,
  runtimeRequestId: Schema.optional(shortText),
  continuationId: Schema.optional(shortText),
});
export type WorkTaskActionReview = typeof WorkTaskActionReview.Type;

// Service authorization and caller identity are independent of these input fields.
export const WorkTaskActionReviewDetailInput = Schema.Struct({ taskId: reviewTaskId, reviewId });
export type WorkTaskActionReviewDetailInput = typeof WorkTaskActionReviewDetailInput.Type;
export const WorkTaskActionReviewDetailResult = Schema.Struct({
  sequence: NonNegativeInt,
  review: WorkTaskActionReview,
});
export type WorkTaskActionReviewDetailResult = typeof WorkTaskActionReviewDetailResult.Type;

export const WorkTaskRequestActionReviewInput = Schema.Struct({
  taskId: reviewTaskId,
  commandId: reviewCommandId,
  expectedDefinitionRevision: PositiveInt,
  expectedActionPolicyRevision: PositiveInt,
  expectedManagerGeneration: PositiveInt,
  ...reviewProposal,
});
export type WorkTaskRequestActionReviewInput = typeof WorkTaskRequestActionReviewInput.Type;
export const WorkTaskRequestActionReviewResult = Schema.Struct({
  sequence: NonNegativeInt,
  review: WorkTaskActionReview,
});
export type WorkTaskRequestActionReviewResult = typeof WorkTaskRequestActionReviewResult.Type;

export const WorkTaskRespondActionReviewInput = Schema.Struct({
  taskId: reviewTaskId,
  commandId: reviewCommandId,
  reviewId,
  expectedReviewRevision: PositiveInt,
  expectedActionPolicyRevision: PositiveInt,
  expectedManagerGeneration: PositiveInt,
  expectedFingerprint: actionFingerprint,
  decision: WorkTaskActionReviewDecision,
  feedback: Schema.optional(shortText),
});
export type WorkTaskRespondActionReviewInput = typeof WorkTaskRespondActionReviewInput.Type;
export const WorkTaskRespondActionReviewResult = Schema.Struct({
  sequence: NonNegativeInt,
  review: WorkTaskActionReview,
});
export type WorkTaskRespondActionReviewResult = typeof WorkTaskRespondActionReviewResult.Type;

// Only dedicated manager runs carry this binding; contributor links do not imply ownership.
export const WorkTaskManagerRunBinding = Schema.Struct({
  taskId: WorkTaskId,
  managerGeneration: PositiveInt,
  definitionRevision: PositiveInt,
  lifecycleRevision: PositiveInt,
  completionPolicyRevision: PositiveInt,
  actionPolicyRevision: PositiveInt,
});
export type WorkTaskManagerRunBinding = typeof WorkTaskManagerRunBinding.Type;

export const WorkTaskActionPayloadIdentity = Schema.Struct({
  action: exactScope.check(Schema.isMaxLength(120)),
  target: exactScope,
  fingerprint: actionFingerprint,
});
export type WorkTaskActionPayloadIdentity = typeof WorkTaskActionPayloadIdentity.Type;

const eventRevisions = {
  definitionRevision: PositiveInt,
  lifecycleRevision: PositiveInt,
  completionPolicyRevision: PositiveInt,
  actionPolicyRevision: PositiveInt,
};
const eventBase = {
  id: EventId,
  taskId: WorkTaskId,
  occurredAt: IsoDateTime,
  attribution: WorkTaskCallerContext,
  ...eventRevisions,
};
const managerIdentity = { managerThreadId: ThreadId, managerGeneration: PositiveInt };
const completionIdentity = {
  ...managerIdentity,
  claimId: shortText,
  evidenceId: shortText,
  observedWorkSequence: NonNegativeInt,
};

// Staged independently of the application event and persistence unions until atomic registration.
export const WorkTaskEvent = Schema.Union([
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.created"),
    projectId: Schema.optional(ProjectId),
    definition: WorkTaskDraftDefinition,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.definition-updated"),
    definition: WorkTaskDraftDefinition,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literals(["task.thread-linked", "task.thread-unlinked"]),
    threadId: ThreadId,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.configuration-updated"),
    allowanceOverride: Schema.NullOr(PositiveInt),
    completionPolicy: WorkTaskCompletionPolicyOverride,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.execution-updated"),
    desiredMode: Schema.Literals(["not_started", "active", "paused", "closed"]),
    previousManagerGeneration: Schema.NullOr(PositiveInt),
    manager: Schema.NullOr(Schema.Struct(managerIdentity)),
    reason: shortText,
    runId: Schema.optional(RunId),
    messageId: Schema.optional(MessageId),
    effectId: Schema.optional(shortText),
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.wait-updated"),
    ...managerIdentity,
    waitId: shortText,
    target: WorkTaskWaitTarget,
    state: Schema.Literals(["registered", "cleared"]),
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.result-recorded"),
    sourceId: shortText,
    summary: shortText,
    results: workTaskReferenceBatch,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.continuation-accepted"),
    ...managerIdentity,
    sourceIds: Schema.Array(shortText).check(Schema.isMinLength(1), Schema.isMaxLength(32)),
    runId: RunId,
    messageId: MessageId,
    effectId: shortText,
    dirtyGeneration: PositiveInt,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.completion-submitted"),
    ...completionIdentity,
    summary: shortText,
    evidence: workTaskReferenceBatch,
  }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.validation-recorded"),
    ...completionIdentity,
    attemptId: shortText,
    validator: WorkTaskLaunchSelection,
    outcome: Schema.Literals(["pass", "fail", "inconclusive", "error"]),
    findings: Schema.Array(shortText).check(Schema.isMaxLength(64)),
  }),
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.completed"), ...completionIdentity }),
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.reopened") }),
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.archived"), archived: Schema.Boolean }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.action-review-requested"),
    attribution: Schema.Struct({ type: Schema.Literal("server") }),
    review: WorkTaskActionReview.mapFields((fields) => ({
      ...fields,
      state: Schema.Struct({ type: Schema.Literal("pending") }),
    })),
  }).check(
    Schema.makeFilter(
      (event) =>
        event.taskId === event.review.taskId &&
        event.definitionRevision === event.review.definitionRevision &&
        event.actionPolicyRevision === event.review.actionPolicyRevision,
    ),
  ),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.action-review-result-recorded"),
    attribution: Schema.Struct({ type: Schema.Literal("server") }),
    reviewId,
    reviewRevision: PositiveInt,
    ...managerIdentity,
    runId: RunId,
    payloadIdentity: WorkTaskActionPayloadIdentity,
    decision: WorkTaskActionReviewDecision,
    human: Schema.Struct({ subject: shortText, sessionId: AuthSessionId }),
    feedback: Schema.optional(shortText),
  }),
]);
export type WorkTaskEvent = typeof WorkTaskEvent.Type;

export const WorkTaskReceipt = Schema.Struct({
  commandId: CommandId,
  taskId: WorkTaskId,
  commandType: shortText,
  sequence: NonNegativeInt,
  ...eventRevisions,
  managerGeneration: Schema.NullOr(PositiveInt),
});
export type WorkTaskReceipt = typeof WorkTaskReceipt.Type;
