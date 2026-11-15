import * as Schema from "effect/Schema";

import {
  AuthSessionId,
  CommandId,
  EventId,
  EnvironmentId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  TurnItemId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

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
    "invalid_page_token",
    "stale_page_token",
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
const resultAttribution = WorkTaskCallerContext.check(
  Schema.makeFilter((caller) => {
    if (caller.type === "server") return true;
    if (caller.type === "user")
      return caller.sessionId.length <= 2_000 && caller.subject.length <= 2_000;
    return (
      caller.threadId.length <= 2_000 &&
      caller.providerSessionId.length <= 2_000 &&
      caller.runId.length <= 2_000
    );
  }),
);
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
  threadId: Schema.optional(referenceThreadId),
  attribution: resultAttribution,
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

const persistedTime = IsoDateTime.check(
  Schema.isMaxLength(2_000),
  Schema.makeFilter(
    (value) => {
      const match =
        /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/.exec(
          value,
        );
      if (!match || !Number.isFinite(Date.parse(value))) return false;
      const [, year, month, day, hour, minute, second, zone] = match;
      if (!year || !month || !day || !hour || !minute || !second || !zone) return false;
      const y = Number(year);
      const m = Number(month);
      const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
      const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
      return (
        m >= 1 &&
        m <= 12 &&
        Number(day) >= 1 &&
        Number(day) <= (days[m - 1] ?? 0) &&
        Number(hour) <= 23 &&
        Number(minute) <= 59 &&
        Number(second) <= 59 &&
        (zone === "Z" || (Number(zone.slice(1, 3)) <= 23 && Number(zone.slice(4, 6)) <= 59))
      );
    },
    { expected: "a valid ISO timestamp" },
  ),
);

const isAtOrBefore = (earlier: string, later: string) => {
  const earlierMillis = Date.parse(earlier);
  const laterMillis = Date.parse(later);
  if (earlierMillis !== laterMillis) return earlierMillis < laterMillis;
  // Date.parse truncates fractional precision after milliseconds.
  const remainder = (value: string) => value.match(/\.(\d+)/)?.[1]?.slice(3) ?? "";
  const left = remainder(earlier);
  const right = remainder(later);
  const width = Math.max(left.length, right.length);
  return left.padEnd(width, "0") <= right.padEnd(width, "0");
};

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

const reviewEventBinding = Schema.Struct({
  taskId: reviewTaskId,
  reviewId,
  reviewRevision: PositiveInt,
  definitionRevision: PositiveInt,
  actionPolicyRevision: PositiveInt,
  payloadIdentity: WorkTaskActionPayloadIdentity,
  ruleId: WorkTaskActionRuleId.check(Schema.isMaxLength(2_000)),
  runtimeRequestId: shortText,
  providerSessionId: ProviderSessionId.check(Schema.isMaxLength(2_000)),
  managerThreadId: referenceThreadId,
  managerGeneration: PositiveInt,
  runId: referenceRunId,
});
const reviewEventBase = {
  ...eventBase,
  id: EventId.check(Schema.isMaxLength(2_000)),
  taskId: reviewTaskId,
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
  binding: reviewEventBinding,
};
const reviewBindingMatches = Schema.makeFilter(
  (event: {
    readonly taskId: string;
    readonly definitionRevision: number;
    readonly actionPolicyRevision: number;
    readonly binding: {
      readonly taskId: string;
      readonly definitionRevision: number;
      readonly actionPolicyRevision: number;
    };
  }) =>
    event.taskId === event.binding.taskId &&
    event.definitionRevision === event.binding.definitionRevision &&
    event.actionPolicyRevision === event.binding.actionPolicyRevision,
);
const reviewInvalidated = Schema.Struct({
  ...reviewEventBase,
  type: Schema.Literal("task.action-review-invalidated"),
  cause: Schema.Literals([
    "payload_changed",
    "policy_changed",
    "generation_changed",
    "run_changed",
    "authorization_changed",
  ]),
  reason: shortText,
}).check(Schema.makeFilter((event) => event.taskId === event.binding.taskId));
const reviewCancelled = Schema.Struct({
  ...reviewEventBase,
  type: Schema.Literal("task.action-review-cancelled"),
  reason: shortText,
  source: Schema.Struct({
    type: Schema.Literal("task.archive"),
    commandId: reviewCommandId,
    human: Schema.Struct({ subject: shortText, sessionId: humanDecision.sessionId }),
  }),
}).check(reviewBindingMatches);
const reviewReleaseBase = {
  ...reviewEventBase,
  type: Schema.Literal("task.action-review-continuation-released"),
  continuationId: shortText,
  reason: shortText,
  decisionEventId: EventId.check(Schema.isMaxLength(2_000)),
};
const reviewContinuationReleased = Schema.Union([
  Schema.Struct({
    ...reviewReleaseBase,
    purpose: Schema.Literal("approved_action"),
    decision: Schema.Literal("approve"),
  }).check(reviewBindingMatches),
  Schema.Struct({
    ...reviewReleaseBase,
    purpose: Schema.Literal("feedback"),
    decision: Schema.Literals(["reject", "request_changes"]),
  }).check(reviewBindingMatches),
]);

export class WorkTaskManagerBindingReplayConflict extends Schema.TaggedError<WorkTaskManagerBindingReplayConflict>()(
  "WorkTaskManagerBindingReplayConflict",
  {
    code: Schema.Literals([
      "binding_identity_conflict",
      "thread_already_bound",
      "current_binding_conflict",
      "binding_not_found",
      "binding_already_retired",
    ]),
    taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
    managerThreadId: referenceThreadId,
    managerGeneration: PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  },
) {}

const managerBindingEventBase = {
  ...eventBase,
  id: EventId.check(Schema.isMaxLength(2_000)),
  taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
  occurredAt: persistedTime,
  attribution: resultAttribution,
  managerThreadId: referenceThreadId,
  managerGeneration: PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
};

const allowanceCount = NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
const allowancePositive = PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
const configuredAllowance = Schema.Struct({
  type: Schema.Literal("configured"),
  cap: allowancePositive,
  consumed: allowanceCount,
}).check(Schema.makeFilter((state) => state.consumed <= state.cap));
export const WorkTaskAllowanceState = Schema.Union([
  Schema.Struct({ type: Schema.Literal("unconfigured"), consumed: allowanceCount }),
  configuredAllowance,
]);
export type WorkTaskAllowanceState = typeof WorkTaskAllowanceState.Type;

export class WorkTaskAllowanceReplayConflict extends Schema.TaggedError<WorkTaskAllowanceReplayConflict>()(
  "WorkTaskAllowanceReplayConflict",
  {
    code: Schema.Literals([
      "prior_state_mismatch",
      "allowance_not_configured",
      "cap_below_consumed",
      "allowance_exhausted",
      "unit_identity_conflict",
      "command_identity_conflict",
    ]),
    taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
    commandId: CommandId.check(Schema.isMaxLength(2_000)),
  },
) {}

const allowanceEventBase = {
  ...eventBase,
  id: EventId.check(Schema.isMaxLength(2_000)),
  taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
  occurredAt: persistedTime,
  commandId: CommandId.check(Schema.isMaxLength(2_000)),
};
const allowanceConfiguration = Schema.Struct({
  ...allowanceEventBase,
  type: Schema.Literal("task.allowance-configured"),
  attribution: Schema.Union([
    Schema.Struct({
      type: Schema.Literal("user"),
      subject: shortText,
      sessionId: humanDecision.sessionId,
    }),
    Schema.Struct({ type: Schema.Literal("server") }),
  ]),
  previous: WorkTaskAllowanceState,
  resulting: configuredAllowance,
}).check(Schema.makeFilter((event) => event.previous.consumed === event.resulting.consumed));
const allowanceTopUp = Schema.Struct({
  ...allowanceEventBase,
  type: Schema.Literal("task.allowance-topped-up"),
  attribution: Schema.Struct({
    type: Schema.Literal("user"),
    subject: shortText,
    sessionId: humanDecision.sessionId,
  }),
  previous: configuredAllowance,
  resulting: configuredAllowance,
}).check(
  Schema.makeFilter(
    (event) =>
      event.previous.consumed === event.resulting.consumed &&
      event.resulting.cap > event.previous.cap,
  ),
);
const allowanceDebit = Schema.Struct({
  ...allowanceEventBase,
  type: Schema.Literal("task.allowance-debited"),
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
  previous: configuredAllowance,
  resulting: configuredAllowance,
  allowanceUnit: allowancePositive,
}).check(
  Schema.makeFilter(
    (event) =>
      event.resulting.cap === event.previous.cap &&
      event.resulting.consumed === event.previous.consumed + 1 &&
      event.allowanceUnit === event.resulting.consumed,
  ),
);

export class WorkTaskWakeReplayConflict extends Schema.TaggedError<WorkTaskWakeReplayConflict>()(
  "WorkTaskWakeReplayConflict",
  {
    code: Schema.Literals([
      "source_identity_conflict",
      "reservation_identity_conflict",
      "command_identity_conflict",
      "allowance_unit_conflict",
      "pending_reservation_conflict",
      "reservation_not_found",
      "claim_identity_conflict",
      "generation_conflict",
      "acknowledgement_conflict",
    ]),
    taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
    reservationId: Schema.NullOr(shortText),
  },
) {}

const wakeSource = Schema.Struct({
  kind: shortText,
  sourceId: shortText,
  resultRef: Schema.NullOr(shortText),
});
const wakeBatch = Schema.Struct({
  sources: Schema.Array(wakeSource).check(
    Schema.isMaxLength(32),
    Schema.makeFilter(
      (sources) =>
        new Set(sources.map((source) => JSON.stringify([source.kind, source.sourceId]))).size ===
        sources.length,
    ),
  ),
  // A non-null watermark requires authoritative reconciliation of sources beyond the batch.
  overflowThroughGeneration: Schema.NullOr(allowancePositive),
});
const wakeEventBase = {
  ...allowanceEventBase,
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
  reservationId: shortText,
  managerGeneration: allowancePositive,
  reservationCommandId: CommandId.check(Schema.isMaxLength(2_000)),
  allowanceUnit: allowancePositive,
};
const wakeGenerations = {
  dirtyGeneration: allowancePositive,
  acknowledgedGeneration: allowanceCount,
};
const wakeGenerationsValid = Schema.makeFilter(
  (event: { readonly dirtyGeneration: number; readonly acknowledgedGeneration: number }) =>
    event.acknowledgedGeneration <= event.dirtyGeneration,
);
const wakeSourceRecorded = Schema.Struct({
  ...allowanceEventBase,
  type: Schema.Literal("task.wake-source-recorded"),
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
  source: wakeSource,
  reservationId: Schema.NullOr(shortText),
  dirtyGeneration: allowancePositive,
});
const wakeReserved = Schema.Struct({
  ...wakeEventBase,
  type: Schema.Literal("task.wake-reserved"),
  ...wakeGenerations,
  pending: wakeBatch,
}).check(
  wakeGenerationsValid,
  Schema.makeFilter(
    (event) =>
      event.pending.overflowThroughGeneration === null ||
      event.pending.overflowThroughGeneration <= event.dirtyGeneration,
  ),
);
const wakeClaimed = Schema.Struct({
  ...wakeEventBase,
  type: Schema.Literal("task.wake-claimed"),
  ...wakeGenerations,
  messageId: MessageId.check(Schema.isMaxLength(2_000)),
  runId: referenceRunId,
  pending: wakeBatch,
}).check(
  wakeGenerationsValid,
  Schema.makeFilter(
    (event) =>
      event.pending.overflowThroughGeneration === null ||
      event.pending.overflowThroughGeneration <= event.dirtyGeneration,
  ),
);
const wakeClaimUncertain = Schema.Struct({
  ...wakeEventBase,
  type: Schema.Literal("task.wake-claim-uncertain"),
  ...wakeGenerations,
  reason: shortText,
}).check(wakeGenerationsValid);
const wakeAcknowledged = Schema.Struct({
  ...wakeEventBase,
  type: Schema.Literal("task.wake-acknowledged"),
  ...wakeGenerations,
  observedDirtyGeneration: allowancePositive,
  previousAcknowledgedGeneration: allowanceCount,
}).check(
  wakeGenerationsValid,
  Schema.makeFilter(
    (event) =>
      event.previousAcknowledgedGeneration <= event.acknowledgedGeneration &&
      event.acknowledgedGeneration <= event.observedDirtyGeneration &&
      event.observedDirtyGeneration <= event.dirtyGeneration,
  ),
);

export class WorkTaskValidationReplayConflict extends Schema.TaggedError<WorkTaskValidationReplayConflict>()(
  "WorkTaskValidationReplayConflict",
  {
    code: Schema.Literals([
      "attempt_identity_conflict",
      "run_identity_conflict",
      "allowance_unit_conflict",
      "binding_conflict",
      "attempt_not_found",
      "phase_conflict",
      "result_conflict",
    ]),
    taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
    attemptId: shortText,
  },
) {}

const validationBase = {
  ...allowanceEventBase,
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
  definitionRevision: allowancePositive,
  completionPolicyRevision: allowancePositive,
  attemptId: shortText,
  managerGeneration: allowancePositive,
  claimId: shortText,
  evidenceId: shortText,
  workWatermark: shortText,
  validator: WorkTaskLaunchSelection.check(
    Schema.makeFilter((selection) => utf8.encode(JSON.stringify(selection)).byteLength <= 16_000),
  ),
  allowanceUnit: allowancePositive,
  createdAt: persistedTime,
};
const validationFinding = Schema.Struct({
  criterionId: WorkTaskCriterionId.check(Schema.isMaxLength(2_000)),
  outcome: Schema.Literals(["pass", "fail", "inconclusive"]),
  reason: shortText,
  evidence: Schema.Array(WorkTaskResultReference).check(Schema.isMaxLength(32)),
});
const forbidden = Schema.optionalKey(Schema.Never);
const validationReserved = Schema.Struct({
  ...validationBase,
  type: Schema.Literal("task.validation-reserved"),
  runId: forbidden,
  startedAt: forbidden,
  finishedAt: forbidden,
  outcome: forbidden,
  findings: forbidden,
  reason: forbidden,
}).check(Schema.makeFilter((event) => event.createdAt === event.occurredAt));
const validationStartedBase = {
  ...validationBase,
  runId: referenceRunId,
  startedAt: persistedTime,
};
const validationStarted = Schema.Struct({
  ...validationStartedBase,
  type: Schema.Literal("task.validation-started"),
  finishedAt: forbidden,
  outcome: forbidden,
  findings: forbidden,
  reason: forbidden,
}).check(
  Schema.makeFilter(
    (event) =>
      event.startedAt === event.occurredAt && isAtOrBefore(event.createdAt, event.startedAt),
  ),
);
const validationResult = Schema.Struct({
  ...validationStartedBase,
  type: Schema.Literal("task.validation-result"),
  finishedAt: persistedTime,
  outcome: Schema.Literals(["pass", "fail", "inconclusive", "error"]),
  findings: Schema.Array(validationFinding).check(Schema.isMaxLength(64)),
  reason: Schema.optional(shortText),
}).check(
  Schema.makeFilter(
    (event) =>
      event.finishedAt === event.occurredAt &&
      isAtOrBefore(event.createdAt, event.startedAt) &&
      isAtOrBefore(event.startedAt, event.finishedAt) &&
      (event.outcome !== "error" || event.reason !== undefined) &&
      new Set(event.findings.map((finding) => finding.criterionId)).size ===
        event.findings.length &&
      (event.outcome !== "pass" ||
        (event.findings.length > 0 &&
          event.findings.every((finding) => finding.outcome === "pass"))) &&
      (event.outcome !== "fail" || event.findings.some((finding) => finding.outcome === "fail")),
  ),
);
const validationUncertain = Schema.Struct({
  ...validationStartedBase,
  type: Schema.Literal("task.validation-uncertain"),
  finishedAt: persistedTime,
  reason: shortText,
  outcome: forbidden,
  findings: forbidden,
}).check(
  Schema.makeFilter(
    (event) =>
      event.finishedAt === event.occurredAt &&
      isAtOrBefore(event.createdAt, event.startedAt) &&
      isAtOrBefore(event.startedAt, event.finishedAt),
  ),
);

const WorkTaskNonReviewRequestedEvent = Schema.Union([
  validationReserved,
  validationStarted,
  validationResult,
  validationUncertain,
  wakeSourceRecorded,
  wakeReserved,
  wakeClaimed,
  wakeClaimUncertain,
  wakeAcknowledged,
  allowanceConfiguration,
  allowanceTopUp,
  allowanceDebit,
  Schema.Struct({ ...managerBindingEventBase, type: Schema.Literal("task.manager-bound") }),
  Schema.Struct({ ...managerBindingEventBase, type: Schema.Literal("task.manager-retired") }),
  reviewInvalidated,
  reviewCancelled,
  reviewContinuationReleased,
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.created"),
    projectId: Schema.optional(ProjectId),
    definition: WorkTaskDraftDefinition,
  }).check(Schema.makeFilter((event) => event.definitionRevision === event.definition.revision)),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.definition-updated"),
    definition: WorkTaskDraftDefinition,
  }).check(Schema.makeFilter((event) => event.definitionRevision === event.definition.revision)),
  Schema.Struct({
    ...eventBase,
    id: EventId.check(Schema.isMaxLength(2_000)),
    taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
    type: Schema.Literal("task.definition-accepted"),
    attribution: Schema.Struct({
      type: Schema.Literal("user"),
      subject: shortText,
      sessionId: humanDecision.sessionId,
    }),
    definition: WorkTaskAcceptedDefinition,
  }).check(Schema.makeFilter((event) => event.definitionRevision === event.definition.revision)),
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
    attribution: resultAttribution,
    sourceId: shortText,
    summary: shortText,
    results: workTaskReferenceBatch,
    contributorThreadId: Schema.optional(referenceThreadId),
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
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.completed"), ...completionIdentity }),
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.reopened") }),
  Schema.Struct({ ...eventBase, type: Schema.Literal("task.archived"), archived: Schema.Boolean }),
  Schema.Struct({
    ...eventBase,
    type: Schema.Literal("task.action-review-result-recorded"),
    attribution: Schema.Struct({ type: Schema.Literal("server") }),
    reviewId,
    reviewRevision: PositiveInt,
    ...managerIdentity,
    managerThreadId: referenceThreadId,
    runId: referenceRunId,
    payloadIdentity: WorkTaskActionPayloadIdentity,
    decision: WorkTaskActionReviewDecision,
    human: Schema.Struct({ subject: shortText, sessionId: humanDecision.sessionId }),
    feedback: Schema.optional(shortText),
  }),
]);
const WorkTaskReviewRequestedEvent = Schema.Struct({
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
);

// Staged independently of the application event and persistence unions until atomic registration.
export const WorkTaskEvent = Schema.Union([
  WorkTaskNonReviewRequestedEvent,
  WorkTaskReviewRequestedEvent,
]);
export type WorkTaskEvent = typeof WorkTaskEvent.Type;

export const WorkTaskPersistedEventContext = Schema.Struct({
  sequence: NonNegativeInt,
  eventId: EventId,
  occurredAt: persistedTime,
  commandId: Schema.NullOr(CommandId),
});
export type WorkTaskPersistedEventContext = typeof WorkTaskPersistedEventContext.Type;

const publishedReview = WorkTaskReviewRequestedEvent.mapFields((fields) => ({
  ...fields,
  review: fields.review.mapFields((reviewFields) => ({
    ...reviewFields,
    runtimeRequestId: shortText,
    createdAt: persistedTime,
  })),
}));
const persistedEvent = Schema.Union([
  Schema.Struct({ event: WorkTaskNonReviewRequestedEvent, context: WorkTaskPersistedEventContext }),
  Schema.Struct({
    event: publishedReview,
    context: WorkTaskPersistedEventContext.mapFields((fields) => ({
      ...fields,
      commandId: reviewCommandId,
    })),
  }),
]);
export const WorkTaskPersistedEvent = persistedEvent.check(
  Schema.makeFilter(({ event, context }) => {
    if (event.id !== context.eventId || event.occurredAt !== context.occurredAt) return false;
    if (
      event.type === "task.validation-reserved" ||
      event.type === "task.validation-started" ||
      event.type === "task.validation-result" ||
      event.type === "task.validation-uncertain" ||
      event.type === "task.wake-source-recorded" ||
      event.type === "task.wake-reserved" ||
      event.type === "task.wake-claimed" ||
      event.type === "task.wake-claim-uncertain" ||
      event.type === "task.wake-acknowledged" ||
      event.type === "task.allowance-configured" ||
      event.type === "task.allowance-topped-up" ||
      event.type === "task.allowance-debited"
    )
      return context.commandId === event.commandId;
    if (event.type === "task.definition-accepted")
      return context.commandId !== null && context.commandId.length <= 2_000;
    if (event.type === "task.action-review-cancelled")
      return context.commandId === event.source.commandId;
    if (event.type !== "task.action-review-requested") return true;
    return (
      event.taskId === event.review.taskId &&
      event.definitionRevision === event.review.definitionRevision &&
      event.actionPolicyRevision === event.review.actionPolicyRevision &&
      event.review.createdSequence === context.sequence &&
      Date.parse(event.review.createdAt) === Date.parse(context.occurredAt) &&
      (event.review.createdAt
        .match(/\.(\d+)/)?.[1]
        ?.slice(3)
        .replace(/0+$/, "") ?? "") ===
        (context.occurredAt
          .match(/\.(\d+)/)?.[1]
          ?.slice(3)
          .replace(/0+$/, "") ?? "")
    );
  }),
);
export type WorkTaskPersistedEvent = typeof WorkTaskPersistedEvent.Type;

export const WorkTaskReceipt = Schema.Struct({
  commandId: CommandId,
  taskId: WorkTaskId,
  commandType: shortText,
  sequence: NonNegativeInt,
  ...eventRevisions,
  managerGeneration: Schema.NullOr(PositiveInt),
});
export type WorkTaskReceipt = typeof WorkTaskReceipt.Type;

const graphCounter = NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
export const WorkTaskGraphRevision = graphCounter.pipe(Schema.brand("WorkTaskGraphRevision"));
export type WorkTaskGraphRevision = typeof WorkTaskGraphRevision.Type;
export const WorkTaskGraphEdgeId = shortText.pipe(Schema.brand("WorkTaskGraphEdgeId"));
export type WorkTaskGraphEdgeId = typeof WorkTaskGraphEdgeId.Type;
export const WorkTaskGraphProposalId = shortText.pipe(Schema.brand("WorkTaskGraphProposalId"));
export type WorkTaskGraphProposalId = typeof WorkTaskGraphProposalId.Type;
export const WorkTaskGraphTaskRef = Schema.Struct({
  environmentId: EnvironmentId.check(Schema.isMaxLength(2_000)),
  taskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
});
export type WorkTaskGraphTaskRef = typeof WorkTaskGraphTaskRef.Type;
const graphEndpoints = Schema.Struct({
  dependent: WorkTaskGraphTaskRef,
  blocker: WorkTaskGraphTaskRef,
});
const distinctGraphEndpoints = Schema.makeFilter(
  (value: typeof graphEndpoints.Type) =>
    value.dependent.environmentId !== value.blocker.environmentId ||
    value.dependent.taskId !== value.blocker.taskId,
  { expected: "different graph endpoints" },
);
const graphUser = Schema.Struct({
  subject: shortText,
  sessionId: AuthSessionId.check(Schema.isMaxLength(2_000)),
});
// These output identities describe provenance; the service must derive and authorize them.
export const WorkTaskGraphProvenance = Schema.Struct({
  eventId: EventId.check(Schema.isMaxLength(2_000)),
  commandId: CommandId.check(Schema.isMaxLength(2_000)),
  occurredAt: persistedTime,
  adoptedBy: graphUser,
});
export type WorkTaskGraphProvenance = typeof WorkTaskGraphProvenance.Type;
export const WorkTaskGraphEdgeRecord = Schema.Struct({
  edgeId: WorkTaskGraphEdgeId,
  ...graphEndpoints.fields,
  createdGraphRevision: WorkTaskGraphRevision.check(Schema.isGreaterThan(0)),
  provenance: WorkTaskGraphProvenance,
}).check(
  distinctGraphEndpoints,
  Schema.makeFilter((value) => value.dependent.environmentId === value.blocker.environmentId, {
    expected: "edge endpoints in one environment",
  }),
);
export type WorkTaskGraphEdgeRecord = typeof WorkTaskGraphEdgeRecord.Type;
export const WorkTaskGraphProposalRecord = Schema.Struct({
  proposalId: WorkTaskGraphProposalId,
  ...graphEndpoints.fields,
  observedGraphRevision: WorkTaskGraphRevision,
  reason: Schema.optional(shortText),
  proposedAt: persistedTime,
  proposedBy: Schema.Union([
    Schema.Struct({ type: Schema.Literal("user"), ...graphUser.fields }),
    Schema.Struct({
      type: Schema.Literal("agent"),
      threadId: ThreadId.check(Schema.isMaxLength(2_000)),
      providerSessionId: ProviderSessionId.check(Schema.isMaxLength(2_000)),
      runId: RunId.check(Schema.isMaxLength(2_000)),
    }),
  ]),
}).check(distinctGraphEndpoints);
export type WorkTaskGraphProposalRecord = typeof WorkTaskGraphProposalRecord.Type;
export const WorkTaskCreateBlockerProposalInput = Schema.Struct({
  ...graphEndpoints.fields,
  observedGraphRevision: WorkTaskGraphRevision,
  reason: Schema.optional(shortText),
}).check(distinctGraphEndpoints);
export type WorkTaskCreateBlockerProposalInput = typeof WorkTaskCreateBlockerProposalInput.Type;
export const WorkTaskCreateBlockerProposalResult = Schema.Struct({
  sequence: graphCounter,
  proposal: WorkTaskGraphProposalRecord,
});
export type WorkTaskCreateBlockerProposalResult = typeof WorkTaskCreateBlockerProposalResult.Type;
export const WorkTaskAddBlockerInput = Schema.Struct({
  ...graphEndpoints.fields,
  expectedGraphRevision: WorkTaskGraphRevision,
}).check(distinctGraphEndpoints);
export type WorkTaskAddBlockerInput = typeof WorkTaskAddBlockerInput.Type;
export const WorkTaskAddBlockerResult = Schema.Struct({
  sequence: graphCounter,
  graphRevision: WorkTaskGraphRevision,
  edge: WorkTaskGraphEdgeRecord,
}).check(
  Schema.makeFilter((value) => value.graphRevision === value.edge.createdGraphRevision, {
    expected: "add result revision matching the created edge",
  }),
);
export type WorkTaskAddBlockerResult = typeof WorkTaskAddBlockerResult.Type;
export const WorkTaskRemoveBlockerInput = Schema.Struct({
  edgeId: WorkTaskGraphEdgeId,
  expectedGraphRevision: WorkTaskGraphRevision,
});
export type WorkTaskRemoveBlockerInput = typeof WorkTaskRemoveBlockerInput.Type;
export const WorkTaskRemoveBlockerResult = Schema.Struct({
  sequence: graphCounter,
  graphRevision: WorkTaskGraphRevision,
  edgeId: WorkTaskGraphEdgeId,
  provenance: WorkTaskGraphProvenance,
});
export type WorkTaskRemoveBlockerResult = typeof WorkTaskRemoveBlockerResult.Type;
export const WorkTaskGraphReadInput = Schema.Struct({
  task: WorkTaskGraphTaskRef,
  direction: Schema.Literals(["blockers", "dependents"]),
  ...pageInput,
});
export type WorkTaskGraphReadInput = typeof WorkTaskGraphReadInput.Type;
export const WorkTaskGraphReadResult = Schema.Struct({
  sequence: graphCounter,
  graphRevision: WorkTaskGraphRevision,
  viewGeneration: graphCounter,
  edges: Schema.Array(WorkTaskGraphEdgeRecord).check(Schema.isMaxLength(100)),
  nextPageToken: Schema.optional(WorkTaskPageToken),
}).check(
  Schema.makeFilter(
    (value) => value.edges.every((edge) => edge.createdGraphRevision <= value.graphRevision),
    {
      expected: "edges created no later than the graph snapshot",
    },
  ),
);
export type WorkTaskGraphReadResult = typeof WorkTaskGraphReadResult.Type;
export const WorkTaskGraphErrorCode = Schema.Literals([
  "graph_revision_conflict",
  "dependency_self_edge",
  "dependency_not_found_or_denied",
  "dependency_foreign_environment",
  "dependency_duplicate",
  "dependency_cycle",
  "dependency_limit_exceeded",
  "invalid_page_token",
  "stale_page_token",
  "unsupported_capability",
]);
export type WorkTaskGraphErrorCode = typeof WorkTaskGraphErrorCode.Type;
export class WorkTaskGraphError extends Schema.TaggedError<WorkTaskGraphError>()(
  "WorkTaskGraphError",
  {
    code: WorkTaskGraphErrorCode,
    message: shortText,
  },
) {}

const groupCounter = NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
export const WorkTaskManagerGroupId = shortText.pipe(Schema.brand("WorkTaskManagerGroupId"));
export type WorkTaskManagerGroupId = typeof WorkTaskManagerGroupId.Type;
export const WorkTaskGroupManagerGeneration = PositiveInt.check(
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
).pipe(Schema.brand("WorkTaskGroupManagerGeneration"));
export type WorkTaskGroupManagerGeneration = typeof WorkTaskGroupManagerGeneration.Type;
export const WorkTaskGroupControlRevision = groupCounter.pipe(
  Schema.brand("WorkTaskGroupControlRevision"),
);
export type WorkTaskGroupControlRevision = typeof WorkTaskGroupControlRevision.Type;
export const WorkTaskGroupMembershipRevision = groupCounter.pipe(
  Schema.brand("WorkTaskGroupMembershipRevision"),
);
export type WorkTaskGroupMembershipRevision = typeof WorkTaskGroupMembershipRevision.Type;
export const WorkTaskManagerGroupRef = Schema.Struct({
  environmentId: EnvironmentId.check(Schema.isMaxLength(2_000)),
  groupId: WorkTaskManagerGroupId,
});
export type WorkTaskManagerGroupRef = typeof WorkTaskManagerGroupRef.Type;
export const WorkTaskGroupManagerRef = Schema.Struct({
  threadId: ThreadId.check(Schema.isMaxLength(2_000)),
  managerGeneration: WorkTaskGroupManagerGeneration,
});
export type WorkTaskGroupManagerRef = typeof WorkTaskGroupManagerRef.Type;
export const WorkTaskManagerGroupControlSnapshot = Schema.Struct({
  group: WorkTaskManagerGroupRef,
  controlRevision: WorkTaskGroupControlRevision,
  membershipRevision: WorkTaskGroupMembershipRevision,
  highestManagerGeneration: groupCounter,
  currentManager: Schema.NullOr(WorkTaskGroupManagerRef),
  dispatchHold: Schema.Literals(["none", "paused", "stopped"]),
}).check(
  Schema.makeFilter(
    (value) =>
      value.currentManager === null ||
      value.currentManager.managerGeneration === value.highestManagerGeneration,
    { expected: "current manager matching the highest accepted group generation" },
  ),
);
export type WorkTaskManagerGroupControlSnapshot = typeof WorkTaskManagerGroupControlSnapshot.Type;
const groupControlGuards = {
  group: WorkTaskManagerGroupRef,
  expectedControlRevision: WorkTaskGroupControlRevision,
  expectedMembershipRevision: WorkTaskGroupMembershipRevision,
  expectedManagerGeneration: Schema.NullOr(WorkTaskGroupManagerGeneration),
  reason: Schema.optional(shortText),
};
export const WorkTaskManagerGroupPauseInput = Schema.Struct(groupControlGuards);
export type WorkTaskManagerGroupPauseInput = typeof WorkTaskManagerGroupPauseInput.Type;
export const WorkTaskManagerGroupResumeInput = Schema.Struct(groupControlGuards);
export type WorkTaskManagerGroupResumeInput = typeof WorkTaskManagerGroupResumeInput.Type;
export const WorkTaskManagerGroupControlResult = Schema.Struct({
  sequence: groupCounter,
  control: WorkTaskManagerGroupControlSnapshot,
});
export type WorkTaskManagerGroupControlResult = typeof WorkTaskManagerGroupControlResult.Type;

const membershipPositive = PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
export const WorkTaskMembershipGeneration = membershipPositive.pipe(
  Schema.brand("WorkTaskMembershipGeneration"),
);
export type WorkTaskMembershipGeneration = typeof WorkTaskMembershipGeneration.Type;
export const WorkTaskMembershipProvenance = Schema.Struct({
  eventId: EventId.check(Schema.isMaxLength(2_000)),
  commandId: CommandId.check(Schema.isMaxLength(2_000)),
  occurredAt: persistedTime,
  performedBy: graphUser,
});
export type WorkTaskMembershipProvenance = typeof WorkTaskMembershipProvenance.Type;
const membershipFields = {
  task: WorkTaskGraphTaskRef,
  group: WorkTaskManagerGroupRef,
  membershipGeneration: WorkTaskMembershipGeneration,
  acceptedDefinitionRevisionAtJoin: membershipPositive,
  joinedMembershipRevision: WorkTaskGroupMembershipRevision.check(Schema.isGreaterThan(0)),
  joined: WorkTaskMembershipProvenance,
};
const membershipEnvironment = Schema.makeFilter(
  (value: { task: WorkTaskGraphTaskRef; group: WorkTaskManagerGroupRef }) =>
    value.task.environmentId === value.group.environmentId,
  { expected: "task and membership group in the same environment" },
);
const currentMembership = Schema.Struct({
  ...membershipFields,
  state: Schema.Literal("current"),
}).check(membershipEnvironment);
const retiredMembership = Schema.Struct({
  ...membershipFields,
  state: Schema.Literal("retired"),
  retiredMembershipRevision: WorkTaskGroupMembershipRevision,
  retired: WorkTaskMembershipProvenance,
}).check(
  membershipEnvironment,
  Schema.makeFilter(
    (value) =>
      value.retiredMembershipRevision > value.joinedMembershipRevision &&
      isAtOrBefore(value.joined.occurredAt, value.retired.occurredAt),
    { expected: "retirement after the join revision and not before its occurrence" },
  ),
);
export const WorkTaskMembershipRecord = Schema.Union([currentMembership, retiredMembership]);
export type WorkTaskMembershipRecord = typeof WorkTaskMembershipRecord.Type;
export const WorkTaskMembershipTaskRevisions = Schema.Struct({
  definitionRevision: membershipPositive,
  acceptedDefinitionRevision: Schema.NullOr(membershipPositive),
  lifecycleRevision: membershipPositive,
  actionPolicyRevision: membershipPositive,
  completionPolicyRevision: membershipPositive,
}).check(
  Schema.makeFilter(
    (value) =>
      value.acceptedDefinitionRevision === null ||
      value.acceptedDefinitionRevision <= value.definitionRevision,
    { expected: "accepted definition evidence no newer than the current definition" },
  ),
);
export type WorkTaskMembershipTaskRevisions = typeof WorkTaskMembershipTaskRevisions.Type;
const sameMembershipTask = (left: WorkTaskGraphTaskRef, right: WorkTaskGraphTaskRef) =>
  left.environmentId === right.environmentId && left.taskId === right.taskId;
const sameMembershipGroup = (left: WorkTaskManagerGroupRef, right: WorkTaskManagerGroupRef) =>
  left.environmentId === right.environmentId && left.groupId === right.groupId;
export const WorkTaskMembershipSnapshot = Schema.Struct({
  task: WorkTaskGraphTaskRef,
  highestMembershipGeneration: groupCounter,
  currentMembership: Schema.NullOr(currentMembership),
  taskRevisions: WorkTaskMembershipTaskRevisions,
}).check(
  Schema.makeFilter(
    (value) =>
      (value.highestMembershipGeneration === 0 ||
        value.taskRevisions.acceptedDefinitionRevision !== null) &&
      (value.currentMembership === null ||
        (sameMembershipTask(value.task, value.currentMembership.task) &&
          value.highestMembershipGeneration === value.currentMembership.membershipGeneration &&
          value.taskRevisions.acceptedDefinitionRevision !== null &&
          value.currentMembership.acceptedDefinitionRevisionAtJoin <=
            value.taskRevisions.acceptedDefinitionRevision)),
    { expected: "current membership matching the task high-water and accepted definition history" },
  ),
);
export type WorkTaskMembershipSnapshot = typeof WorkTaskMembershipSnapshot.Type;
const membershipPageInput = {
  limit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))),
  pageToken: Schema.optional(WorkTaskPageToken),
};
const membershipPage = {
  sequence: groupCounter,
  viewGeneration: groupCounter,
  nextPageToken: Schema.optional(WorkTaskPageToken),
};
export const WorkTaskGroupMemberReadInput = Schema.Struct({
  group: WorkTaskManagerGroupRef,
  ...membershipPageInput,
});
export type WorkTaskGroupMemberReadInput = typeof WorkTaskGroupMemberReadInput.Type;
export const WorkTaskGroupMemberReadResult = Schema.Struct({
  ...membershipPage,
  control: WorkTaskManagerGroupControlSnapshot,
  members: Schema.Array(currentMembership).check(Schema.isMaxLength(100)),
}).check(
  Schema.makeFilter(
    (value) =>
      new Set(value.members.map((member) => member.task.taskId)).size === value.members.length &&
      value.members.every(
        (member) =>
          sameMembershipGroup(member.group, value.control.group) &&
          member.joinedMembershipRevision <= value.control.membershipRevision,
      ),
    { expected: "unique current members belonging to the group control snapshot" },
  ),
);
export type WorkTaskGroupMemberReadResult = typeof WorkTaskGroupMemberReadResult.Type;
const sameCurrentMembership = (
  left: typeof currentMembership.Type,
  right: typeof currentMembership.Type,
) =>
  sameMembershipTask(left.task, right.task) &&
  sameMembershipGroup(left.group, right.group) &&
  left.membershipGeneration === right.membershipGeneration &&
  left.acceptedDefinitionRevisionAtJoin === right.acceptedDefinitionRevisionAtJoin &&
  left.joinedMembershipRevision === right.joinedMembershipRevision &&
  left.joined.eventId === right.joined.eventId &&
  left.joined.commandId === right.joined.commandId &&
  left.joined.occurredAt === right.joined.occurredAt &&
  left.joined.performedBy.subject === right.joined.performedBy.subject &&
  left.joined.performedBy.sessionId === right.joined.performedBy.sessionId;
export const WorkTaskMembershipHistoryInput = Schema.Struct({
  task: WorkTaskGraphTaskRef,
  ...membershipPageInput,
});
export type WorkTaskMembershipHistoryInput = typeof WorkTaskMembershipHistoryInput.Type;
export const WorkTaskMembershipHistoryResult = Schema.Struct({
  ...membershipPage,
  snapshot: WorkTaskMembershipSnapshot,
  memberships: Schema.Array(WorkTaskMembershipRecord).check(Schema.isMaxLength(100)),
}).check(
  Schema.makeFilter(
    ({ snapshot, memberships }) =>
      memberships.every((member, index) => {
        if (
          !sameMembershipTask(member.task, snapshot.task) ||
          member.membershipGeneration > snapshot.highestMembershipGeneration ||
          snapshot.taskRevisions.acceptedDefinitionRevision === null ||
          member.acceptedDefinitionRevisionAtJoin >
            snapshot.taskRevisions.acceptedDefinitionRevision ||
          (index > 0 && memberships[index - 1]!.membershipGeneration <= member.membershipGeneration)
        )
          return false;
        const current = snapshot.currentMembership;
        if (member.state === "current")
          return current !== null && sameCurrentMembership(member, current);
        return current === null || member.membershipGeneration !== current.membershipGeneration;
      }),
    {
      expected:
        "descending distinct task history consistent with current membership and acceptance",
    },
  ),
);
export type WorkTaskMembershipHistoryResult = typeof WorkTaskMembershipHistoryResult.Type;
const membershipGroupGuard = Schema.Struct({
  group: WorkTaskManagerGroupRef,
  expectedControlRevision: WorkTaskGroupControlRevision,
  expectedMembershipRevision: WorkTaskGroupMembershipRevision,
  expectedManagerGeneration: Schema.NullOr(WorkTaskGroupManagerGeneration),
});
export const WorkTaskChangeMembershipInput = Schema.Struct({
  task: WorkTaskGraphTaskRef,
  expectedTaskRevisions: WorkTaskMembershipTaskRevisions,
  expectedMembershipGeneration: groupCounter,
  sourceGroup: Schema.NullOr(membershipGroupGuard),
  destinationGroup: Schema.NullOr(membershipGroupGuard),
  reason: Schema.optional(shortText),
}).check(
  Schema.makeFilter(
    ({
      task,
      expectedTaskRevisions,
      expectedMembershipGeneration,
      sourceGroup,
      destinationGroup,
    }) =>
      (sourceGroup !== null || destinationGroup !== null) &&
      (sourceGroup === null ||
        (sourceGroup.group.environmentId === task.environmentId &&
          expectedMembershipGeneration > 0)) &&
      (destinationGroup === null ||
        (destinationGroup.group.environmentId === task.environmentId &&
          expectedTaskRevisions.acceptedDefinitionRevision !== null)) &&
      (sourceGroup === null ||
        destinationGroup === null ||
        !sameMembershipGroup(sourceGroup.group, destinationGroup.group)),
    {
      expected: "explicit same-environment join, move or removal with recoverable revision guards",
    },
  ),
);
export type WorkTaskChangeMembershipInput = typeof WorkTaskChangeMembershipInput.Type;
export const WorkTaskChangeMembershipResult = Schema.Struct({
  sequence: groupCounter,
  snapshot: WorkTaskMembershipSnapshot,
  controls: Schema.Array(WorkTaskManagerGroupControlSnapshot).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(2),
  ),
}).check(
  Schema.makeFilter(
    ({ snapshot, controls }) =>
      controls.every((control) => control.group.environmentId === snapshot.task.environmentId) &&
      new Set(controls.map((control) => control.group.groupId)).size === controls.length &&
      (snapshot.currentMembership === null ||
        controls.some(
          (control) =>
            sameMembershipGroup(control.group, snapshot.currentMembership!.group) &&
            snapshot.currentMembership!.joinedMembershipRevision <= control.membershipRevision,
        )),
    { expected: "distinct affected controls including the resulting current membership" },
  ),
);
export type WorkTaskChangeMembershipResult = typeof WorkTaskChangeMembershipResult.Type;

export const WorkTaskWorkClaimId = shortText.pipe(Schema.brand("WorkTaskWorkClaimId"));
export type WorkTaskWorkClaimId = typeof WorkTaskWorkClaimId.Type;
const workPositive = PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
const acceptedWorkRevisions = Schema.Struct({
  ...WorkTaskMembershipTaskRevisions.fields,
  acceptedDefinitionRevision: workPositive,
}).check(
  Schema.makeFilter((value) => value.acceptedDefinitionRevision <= value.definitionRevision, {
    expected: "accepted work definition no newer than the current draft",
  }),
);
export const WorkTaskRunBindingV2 = Schema.Struct({
  version: Schema.Literal(2),
  workClaimId: WorkTaskWorkClaimId,
  task: WorkTaskGraphTaskRef,
  group: WorkTaskManagerGroupRef,
  manager: WorkTaskGroupManagerRef,
  membershipGeneration: WorkTaskMembershipGeneration,
  membershipRevision: WorkTaskGroupMembershipRevision.check(Schema.isGreaterThan(0)),
  controlRevision: WorkTaskGroupControlRevision,
  graphRevision: WorkTaskGraphRevision,
  projectId: ProjectId.check(Schema.isMaxLength(2_000)),
  providerInstanceId: ProviderInstanceId,
  taskRevisions: acceptedWorkRevisions,
  commandId: CommandId.check(Schema.isMaxLength(2_000)),
  runId: RunId.check(Schema.isMaxLength(2_000)),
  messageId: MessageId.check(Schema.isMaxLength(2_000)),
  effectId: shortText,
}).check(membershipEnvironment);
export type WorkTaskRunBindingV2 = typeof WorkTaskRunBindingV2.Type;
export const WorkTaskPrerequisiteCompletionWitness = Schema.Struct({
  edgeId: WorkTaskGraphEdgeId,
  blocker: WorkTaskGraphTaskRef,
  completionEventId: EventId.check(Schema.isMaxLength(2_000)),
  completionSequence: groupCounter,
});
export type WorkTaskPrerequisiteCompletionWitness =
  typeof WorkTaskPrerequisiteCompletionWitness.Type;
const initialWorkOrigin = Schema.Struct({
  type: Schema.Literal("initial_start"),
  startCommandId: CommandId.check(Schema.isMaxLength(2_000)),
  startEventId: EventId.check(Schema.isMaxLength(2_000)),
  startSequence: groupCounter,
  reservationId: forbidden,
  reservationCommandId: forbidden,
  allowanceUnit: forbidden,
  observedDirtyGeneration: forbidden,
  acknowledgedGeneration: forbidden,
});
const automaticWorkOrigin = Schema.Struct({
  type: Schema.Literal("automatic_followup"),
  reservationId: shortText,
  reservationCommandId: CommandId.check(Schema.isMaxLength(2_000)),
  allowanceUnit: workPositive,
  observedDirtyGeneration: workPositive,
  acknowledgedGeneration: groupCounter,
  startCommandId: forbidden,
  startEventId: forbidden,
  startSequence: forbidden,
}).check(
  Schema.makeFilter((value) => value.acknowledgedGeneration <= value.observedDirtyGeneration, {
    expected: "acknowledged work no later than the observed dirty generation",
  }),
);
export const WorkTaskAcceptedWorkClaim = Schema.Struct({
  binding: WorkTaskRunBindingV2,
  acceptedEventId: EventId.check(Schema.isMaxLength(2_000)),
  acceptedSequence: groupCounter,
  acceptedAt: persistedTime,
  sourceEventId: EventId.check(Schema.isMaxLength(2_000)),
  sourceSequence: groupCounter,
  observedWorkSequence: groupCounter,
  prerequisites: Schema.Array(WorkTaskPrerequisiteCompletionWitness).check(Schema.isMaxLength(64)),
  launchOrigin: Schema.Union([initialWorkOrigin, automaticWorkOrigin]),
}).check(
  Schema.makeFilter(
    ({
      binding,
      acceptedSequence,
      sourceSequence,
      observedWorkSequence,
      prerequisites,
      launchOrigin,
    }) =>
      sourceSequence <= acceptedSequence &&
      observedWorkSequence <= acceptedSequence &&
      (launchOrigin.type !== "initial_start" || launchOrigin.startSequence <= acceptedSequence) &&
      new Set(prerequisites.map((witness) => witness.edgeId)).size === prerequisites.length &&
      new Set(prerequisites.map((witness) => witness.blocker.taskId)).size ===
        prerequisites.length &&
      prerequisites.every(
        (witness) =>
          witness.blocker.environmentId === binding.task.environmentId &&
          !sameMembershipTask(witness.blocker, binding.task) &&
          witness.completionSequence <= acceptedSequence,
      ),
    { expected: "bounded distinct prerequisite and launch evidence no later than work acceptance" },
  ),
);
export type WorkTaskAcceptedWorkClaim = typeof WorkTaskAcceptedWorkClaim.Type;

const graphEventFields = {
  id: EventId.check(Schema.isMaxLength(2_000)),
  environmentId: EnvironmentId.check(Schema.isMaxLength(2_000)),
  occurredAt: persistedTime,
  commandId: CommandId.check(Schema.isMaxLength(2_000)),
};
const graphChangeFields = {
  ...graphEventFields,
  previousGraphRevision: WorkTaskGraphRevision,
  graphRevision: WorkTaskGraphRevision,
  edge: WorkTaskGraphEdgeRecord,
};
const graphChangeConsistent = Schema.makeFilter(
  (value: {
    previousGraphRevision: WorkTaskGraphRevision;
    graphRevision: WorkTaskGraphRevision;
    environmentId: EnvironmentId;
    edge: WorkTaskGraphEdgeRecord;
  }) =>
    value.graphRevision === value.previousGraphRevision + 1 &&
    value.edge.dependent.environmentId === value.environmentId &&
    value.edge.blocker.environmentId === value.environmentId,
  { expected: "one topology revision advance with local edge endpoints" },
);
const graphProvenanceMatches = (
  event: { id: EventId; commandId: CommandId; occurredAt: string },
  provenance: WorkTaskGraphProvenance,
) =>
  event.id === provenance.eventId &&
  event.commandId === provenance.commandId &&
  event.occurredAt === provenance.occurredAt;
const graphEdgeAdopted = Schema.Struct({
  type: Schema.Literal("task.graph-edge-adopted"),
  ...graphChangeFields,
}).check(
  graphChangeConsistent,
  Schema.makeFilter(
    (value) =>
      value.edge.createdGraphRevision === value.graphRevision &&
      graphProvenanceMatches(value, value.edge.provenance),
    { expected: "adoption revision and event provenance" },
  ),
);
const graphEdgeRemoved = Schema.Struct({
  type: Schema.Literal("task.graph-edge-removed"),
  ...graphChangeFields,
  provenance: WorkTaskGraphProvenance,
}).check(
  graphChangeConsistent,
  Schema.makeFilter(
    (value) =>
      value.edge.createdGraphRevision <= value.previousGraphRevision &&
      value.id !== value.edge.provenance.eventId &&
      graphProvenanceMatches(value, value.provenance),
    { expected: "historical adoption and distinct removal event provenance" },
  ),
);
const graphProposalRecorded = Schema.Struct({
  type: Schema.Literal("task.graph-proposal-recorded"),
  ...graphEventFields,
  graphRevision: WorkTaskGraphRevision,
  proposal: WorkTaskGraphProposalRecord,
}).check(
  Schema.makeFilter(
    (value) =>
      value.proposal.dependent.environmentId === value.environmentId &&
      value.proposal.observedGraphRevision <= value.graphRevision &&
      value.proposal.proposedAt === value.occurredAt,
    { expected: "local inert proposal with observed revision and event time" },
  ),
);
export const WorkTaskGraphEvent = Schema.Union([
  graphEdgeAdopted,
  graphEdgeRemoved,
  graphProposalRecorded,
]);
export type WorkTaskGraphEvent = typeof WorkTaskGraphEvent.Type;
export const WorkTaskPersistedGraphEvent = Schema.Struct({
  event: WorkTaskGraphEvent,
  context: Schema.Struct({
    sequence: graphCounter,
    eventId: graphEventFields.id,
    occurredAt: persistedTime,
    commandId: graphEventFields.commandId,
  }),
}).check(
  Schema.makeFilter(
    (value) =>
      value.context.eventId === value.event.id &&
      value.context.commandId === value.event.commandId &&
      value.context.occurredAt === value.event.occurredAt,
    { expected: "exact persisted graph event identity, command and time" },
  ),
);
export type WorkTaskPersistedGraphEvent = typeof WorkTaskPersistedGraphEvent.Type;

const groupEventFields = { ...graphEventFields, performedBy: graphUser };
const sameGroupManager = (
  left: WorkTaskGroupManagerRef | null,
  right: WorkTaskGroupManagerRef | null,
) =>
  left === null
    ? right === null
    : right !== null &&
      left.threadId === right.threadId &&
      left.managerGeneration === right.managerGeneration;
const sameGroupControl = (
  left: WorkTaskManagerGroupControlSnapshot,
  right: WorkTaskManagerGroupControlSnapshot,
) =>
  sameMembershipGroup(left.group, right.group) &&
  left.controlRevision === right.controlRevision &&
  left.membershipRevision === right.membershipRevision &&
  left.highestManagerGeneration === right.highestManagerGeneration &&
  sameGroupManager(left.currentManager, right.currentManager) &&
  left.dispatchHold === right.dispatchHold;
const groupTransitionFields = {
  previousControl: WorkTaskManagerGroupControlSnapshot,
  control: WorkTaskManagerGroupControlSnapshot,
};
const localGroupTransition = Schema.makeFilter(
  (value: {
    environmentId: EnvironmentId;
    previousControl: WorkTaskManagerGroupControlSnapshot;
    control: WorkTaskManagerGroupControlSnapshot;
  }) =>
    sameMembershipGroup(value.previousControl.group, value.control.group) &&
    value.control.group.environmentId === value.environmentId,
  { expected: "one local group transition" },
);
const membershipControlChange = (
  previous: WorkTaskManagerGroupControlSnapshot,
  current: WorkTaskManagerGroupControlSnapshot,
) =>
  sameMembershipGroup(previous.group, current.group) &&
  current.membershipRevision === previous.membershipRevision + 1 &&
  current.controlRevision === previous.controlRevision &&
  current.highestManagerGeneration === previous.highestManagerGeneration &&
  sameGroupManager(current.currentManager, previous.currentManager) &&
  current.dispatchHold === previous.dispatchHold;
const groupCreated = Schema.Struct({
  ...groupEventFields,
  type: Schema.Literal("task-group.created"),
  control: WorkTaskManagerGroupControlSnapshot,
}).check(
  Schema.makeFilter(
    ({ environmentId, control }) =>
      control.group.environmentId === environmentId &&
      control.controlRevision === 0 &&
      control.membershipRevision === 0 &&
      control.highestManagerGeneration === 0 &&
      control.currentManager === null &&
      control.dispatchHold === "none",
    { expected: "empty initial group control" },
  ),
);
const groupManagerChanged = Schema.Struct({
  ...groupEventFields,
  type: Schema.Literal("task-group.manager-changed"),
  ...groupTransitionFields,
  reason: Schema.optional(shortText),
}).check(
  localGroupTransition,
  Schema.makeFilter(
    ({ previousControl: previous, control }) =>
      control.controlRevision === previous.controlRevision + 1 &&
      control.membershipRevision === previous.membershipRevision &&
      control.dispatchHold === previous.dispatchHold &&
      (control.currentManager === null
        ? previous.currentManager !== null &&
          control.highestManagerGeneration === previous.highestManagerGeneration
        : control.highestManagerGeneration === previous.highestManagerGeneration + 1 &&
          control.currentManager.threadId !== previous.currentManager?.threadId),
    { expected: "manager bind/retirement preserving independent group domains" },
  ),
);
const groupHoldChanged = Schema.Struct({
  ...groupEventFields,
  type: Schema.Literal("task-group.hold-changed"),
  ...groupTransitionFields,
  reason: Schema.optional(shortText),
}).check(
  localGroupTransition,
  Schema.makeFilter(
    ({ previousControl: previous, control }) =>
      control.controlRevision === previous.controlRevision + 1 &&
      control.membershipRevision === previous.membershipRevision &&
      control.highestManagerGeneration === previous.highestManagerGeneration &&
      sameGroupManager(control.currentManager, previous.currentManager) &&
      control.dispatchHold !== previous.dispatchHold,
    { expected: "changed group hold intent preserving manager and membership" },
  ),
);
const groupMembershipChanged = Schema.Struct({
  ...groupEventFields,
  type: Schema.Literal("task-group.membership-changed"),
  rootEventId: graphEventFields.id,
  task: WorkTaskGraphTaskRef,
  ...groupTransitionFields,
}).check(
  localGroupTransition,
  Schema.makeFilter(
    (value) =>
      value.task.environmentId === value.environmentId &&
      value.rootEventId !== value.id &&
      membershipControlChange(value.previousControl, value.control),
    { expected: "task-linked membership-only group companion" },
  ),
);
export const WorkTaskGroupControlEvent = Schema.Union([
  groupCreated,
  groupManagerChanged,
  groupHoldChanged,
  groupMembershipChanged,
]);
export type WorkTaskGroupControlEvent = typeof WorkTaskGroupControlEvent.Type;
const membershipGroupTransition = Schema.Struct({
  companionEventId: graphEventFields.id,
  ...groupTransitionFields,
}).check(
  Schema.makeFilter((value) => membershipControlChange(value.previousControl, value.control), {
    expected: "one group membership revision advance",
  }),
);
const membershipRevisionKeys = [
  "definitionRevision",
  "acceptedDefinitionRevision",
  "lifecycleRevision",
  "actionPolicyRevision",
  "completionPolicyRevision",
] as const;
const sameRecordedPrincipal = (left: typeof graphUser.Type, right: typeof graphUser.Type) =>
  left.subject === right.subject && left.sessionId === right.sessionId;
const membershipProvenanceMatches = (
  event: {
    id: EventId;
    commandId: CommandId;
    occurredAt: string;
    performedBy: typeof graphUser.Type;
  },
  provenance: WorkTaskMembershipProvenance,
) =>
  graphProvenanceMatches(event, { ...provenance, adoptedBy: provenance.performedBy }) &&
  sameRecordedPrincipal(event.performedBy, provenance.performedBy);
export const WorkTaskMembershipChangedEvent = Schema.Struct({
  ...groupEventFields,
  type: Schema.Literal("task.membership-changed"),
  previousSnapshot: WorkTaskMembershipSnapshot,
  snapshot: WorkTaskMembershipSnapshot,
  retiredMembership: Schema.NullOr(retiredMembership),
  groupTransitions: Schema.Array(membershipGroupTransition).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(2),
  ),
  reason: Schema.optional(shortText),
}).check(
  Schema.makeFilter(
    (event) => {
      const {
        previousSnapshot: previous,
        snapshot,
        groupTransitions,
        retiredMembership: retired,
      } = event;
      const source = previous.currentMembership;
      const destination = snapshot.currentMembership;
      if (
        !sameMembershipTask(previous.task, snapshot.task) ||
        snapshot.task.environmentId !== event.environmentId ||
        !membershipRevisionKeys.every(
          (key) => previous.taskRevisions[key] === snapshot.taskRevisions[key],
        ) ||
        (source === null && destination === null) ||
        (source !== null &&
          destination !== null &&
          sameMembershipGroup(source.group, destination.group)) ||
        snapshot.highestMembershipGeneration !==
          previous.highestMembershipGeneration + (destination === null ? 0 : 1)
      )
        return false;
      const groups = [source?.group, destination?.group].filter((group) => group !== undefined);
      if (
        groupTransitions.length !== groups.length ||
        new Set(groupTransitions.map((transition) => transition.control.group.groupId)).size !==
          groups.length ||
        new Set(groupTransitions.map((transition) => transition.companionEventId)).size !==
          groups.length ||
        groupTransitions.some(
          (transition) =>
            transition.companionEventId === event.id ||
            transition.control.group.environmentId !== event.environmentId ||
            !groups.some((group) => sameMembershipGroup(group, transition.control.group)),
        )
      )
        return false;
      if (source === null) {
        if (retired !== null) return false;
      } else {
        const transition = groupTransitions.find((entry) =>
          sameMembershipGroup(entry.control.group, source.group),
        );
        if (
          retired === null ||
          transition === undefined ||
          event.id === source.joined.eventId ||
          groupTransitions.some((entry) => entry.companionEventId === source.joined.eventId) ||
          !sameCurrentMembership(source, { ...retired, state: "current" }) ||
          retired.retiredMembershipRevision !== transition.control.membershipRevision ||
          !membershipProvenanceMatches(event, retired.retired)
        )
          return false;
      }
      if (destination !== null) {
        const transition = groupTransitions.find((entry) =>
          sameMembershipGroup(entry.control.group, destination.group),
        );
        if (
          transition === undefined ||
          destination.acceptedDefinitionRevisionAtJoin !==
            snapshot.taskRevisions.acceptedDefinitionRevision ||
          destination.joinedMembershipRevision !== transition.control.membershipRevision ||
          !membershipProvenanceMatches(event, destination.joined)
        )
          return false;
      }
      return true;
    },
    {
      expected: "task-global membership transition with exact original and affected group evidence",
    },
  ),
);
export type WorkTaskMembershipChangedEvent = typeof WorkTaskMembershipChangedEvent.Type;
export const WorkTaskMembershipEventBatch = Schema.Struct({
  event: WorkTaskMembershipChangedEvent,
  companions: Schema.Array(groupMembershipChanged).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(2),
  ),
}).check(
  Schema.makeFilter(
    ({ event, companions }) =>
      companions.length === event.groupTransitions.length &&
      new Set(companions.map((companion) => companion.id)).size === companions.length &&
      companions.every((companion) => {
        const transition = event.groupTransitions.find(
          (entry) => entry.companionEventId === companion.id,
        );
        return (
          transition !== undefined &&
          companion.rootEventId === event.id &&
          sameMembershipTask(companion.task, event.snapshot.task) &&
          companion.environmentId === event.environmentId &&
          companion.commandId === event.commandId &&
          companion.occurredAt === event.occurredAt &&
          sameRecordedPrincipal(companion.performedBy, event.performedBy) &&
          sameGroupControl(companion.previousControl, transition.previousControl) &&
          sameGroupControl(companion.control, transition.control)
        );
      }),
    { expected: "complete exact group companions for one task-owned membership mutation" },
  ),
);
export type WorkTaskMembershipEventBatch = typeof WorkTaskMembershipEventBatch.Type;
const groupEventContext = Schema.Struct({
  sequence: groupCounter,
  eventId: graphEventFields.id,
  occurredAt: persistedTime,
  commandId: graphEventFields.commandId,
});
const exactGroupEventContext = Schema.makeFilter(
  (value: {
    event: { id: EventId; commandId: CommandId; occurredAt: string };
    context: typeof groupEventContext.Type;
  }) =>
    value.context.eventId === value.event.id &&
    value.context.commandId === value.event.commandId &&
    value.context.occurredAt === value.event.occurredAt,
  { expected: "exact persisted group or membership event identity, command and time" },
);
export const WorkTaskPersistedGroupControlEvent = Schema.Struct({
  event: WorkTaskGroupControlEvent,
  context: groupEventContext,
}).check(exactGroupEventContext);
export type WorkTaskPersistedGroupControlEvent = typeof WorkTaskPersistedGroupControlEvent.Type;
export const WorkTaskPersistedMembershipEvent = Schema.Struct({
  event: WorkTaskMembershipChangedEvent,
  context: groupEventContext,
}).check(exactGroupEventContext);
export type WorkTaskPersistedMembershipEvent = typeof WorkTaskPersistedMembershipEvent.Type;

const coordinationFields = {
  ...graphEventFields,
  attribution: Schema.Struct({ type: Schema.Literal("server") }),
};
const journalSource = { sourceEventId: graphEventFields.id, sourceSequence: groupCounter };
const claimReference = Schema.Struct({
  binding: WorkTaskRunBindingV2,
  acceptedEventId: graphEventFields.id,
  acceptedSequence: groupCounter,
  acceptedAt: persistedTime,
});
const retainedClaimFields = {
  ...coordinationFields,
  claim: claimReference,
  launchOrigin: forbidden,
  reservationId: forbidden,
  reservationCommandId: forbidden,
  allowanceUnit: forbidden,
};
const retainedClaimLocal = Schema.makeFilter(
  (event: { id: EventId; environmentId: EnvironmentId; claim: typeof claimReference.Type }) =>
    event.environmentId === event.claim.binding.task.environmentId &&
    event.id !== event.claim.acceptedEventId,
  { expected: "original local work claim and a distinct subsequent event" },
);
const workAccepted = Schema.Struct({
  ...coordinationFields,
  type: Schema.Literal("task-group.work-accepted"),
  claim: WorkTaskAcceptedWorkClaim,
  previousActiveClaimId: Schema.Null,
  control: WorkTaskManagerGroupControlSnapshot,
  membership: WorkTaskMembershipSnapshot,
}).check(
  Schema.makeFilter(
    (event) => {
      const { binding } = event.claim;
      const member = event.membership.currentMembership;
      return (
        event.environmentId === binding.task.environmentId &&
        event.id === event.claim.acceptedEventId &&
        event.commandId === binding.commandId &&
        event.occurredAt === event.claim.acceptedAt &&
        event.control.dispatchHold === "none" &&
        sameMembershipGroup(event.control.group, binding.group) &&
        sameGroupManager(event.control.currentManager, binding.manager) &&
        event.control.controlRevision === binding.controlRevision &&
        event.control.membershipRevision === binding.membershipRevision &&
        sameMembershipTask(event.membership.task, binding.task) &&
        member !== null &&
        sameMembershipGroup(member.group, binding.group) &&
        member.membershipGeneration === binding.membershipGeneration &&
        member.joinedMembershipRevision <= binding.membershipRevision &&
        membershipRevisionKeys.every(
          (key) => event.membership.taskRevisions[key] === binding.taskRevisions[key],
        )
      );
    },
    {
      expected:
        "accepted work matching its observed control, membership and exact acceptance context",
    },
  ),
);
const workUncertain = Schema.Struct({
  ...retainedClaimFields,
  type: Schema.Literal("task-group.work-uncertain"),
  reason: shortText,
  ...journalSource,
}).check(retainedClaimLocal);
const journalEventReference = <T extends string>(type: T) =>
  Schema.Struct({
    type: Schema.Literal(type),
    eventId: graphEventFields.id,
    sequence: groupCounter,
  });
const terminalRunReference = journalEventReference("run.updated");
const terminalAttemptReference = journalEventReference("run-attempt.updated");
const interruptReference = Schema.Struct({
  ...journalEventReference("turn-item.updated").fields,
  turnItemId: TurnItemId.check(Schema.isMaxLength(2_000)),
});
const releaseEvidence = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("not_started"),
    terminalRunEvent: terminalRunReference,
    settlementCommandId: graphEventFields.commandId,
  }),
  Schema.Struct({
    type: Schema.Literal("run_settled"),
    terminalRunEvent: terminalRunReference,
    attempt: Schema.Struct({
      attemptId: RunAttemptId.check(Schema.isMaxLength(2_000)),
      providerThreadId: ProviderThreadId.check(Schema.isMaxLength(2_000)),
      providerTurnId: Schema.NullOr(ProviderTurnId.check(Schema.isMaxLength(2_000))),
      terminalAttemptEvent: terminalAttemptReference,
    }),
    interruptResult: Schema.optional(interruptReference),
    settlementCommandId: Schema.optional(graphEventFields.commandId),
  }),
]);
const workReleased = Schema.Struct({
  ...retainedClaimFields,
  type: Schema.Literal("task-group.work-released"),
  observedSequence: groupCounter,
  releaseEvidence,
}).check(retainedClaimLocal);
const dirtyMembership = Schema.Struct({
  group: WorkTaskManagerGroupRef,
  membershipGeneration: WorkTaskMembershipGeneration,
});
const dirtyFields = {
  ...coordinationFields,
  ...journalSource,
  task: WorkTaskGraphTaskRef,
  membership: Schema.NullOr(dirtyMembership),
};
const dirtyScope = Schema.makeFilter(
  (event: {
    environmentId: EnvironmentId;
    task: WorkTaskGraphTaskRef;
    membership: typeof dirtyMembership.Type | null;
  }) =>
    event.task.environmentId === event.environmentId &&
    (event.membership === null || event.membership.group.environmentId === event.environmentId),
  { expected: "local dirty task and recorded membership epoch" },
);
const dirtyRecorded = Schema.Struct({
  ...dirtyFields,
  type: Schema.Literal("task.dirty-source-recorded"),
  previousDirtyGeneration: groupCounter,
  dirtyGeneration: groupCounter,
  acknowledgedGeneration: groupCounter,
}).check(
  dirtyScope,
  Schema.makeFilter(
    (event) =>
      event.dirtyGeneration === event.previousDirtyGeneration + 1 &&
      event.acknowledgedGeneration <= event.previousDirtyGeneration,
    { expected: "one dirty increment preserving acknowledgement" },
  ),
);
const dirtyAcknowledged = Schema.Struct({
  ...dirtyFields,
  type: Schema.Literal("task.dirty-acknowledged"),
  previousAcknowledgedGeneration: groupCounter,
  acknowledgedGeneration: groupCounter,
  observedDirtyGeneration: groupCounter,
  dirtyGeneration: groupCounter,
}).check(
  dirtyScope,
  Schema.makeFilter(
    (event) =>
      event.previousAcknowledgedGeneration <= event.acknowledgedGeneration &&
      event.acknowledgedGeneration <= event.observedDirtyGeneration &&
      event.observedDirtyGeneration <= event.dirtyGeneration,
    { expected: "acknowledgement bounded by observed dirty work" },
  ),
);
const readyFields = {
  ...coordinationFields,
  ...journalSource,
  type: Schema.Literal("task.ready-sequence-updated"),
  task: WorkTaskGraphTaskRef,
  ...dirtyMembership.fields,
  basis: Schema.Struct({
    taskRevisions: WorkTaskMembershipTaskRevisions,
    graphRevision: WorkTaskGraphRevision,
    controlRevision: WorkTaskGroupControlRevision,
    membershipRevision: WorkTaskGroupMembershipRevision.check(Schema.isGreaterThan(0)),
    managerGeneration: Schema.NullOr(WorkTaskGroupManagerGeneration),
  }),
  previousOldestReadySequence: Schema.NullOr(groupCounter),
  oldestReadySequence: Schema.NullOr(groupCounter),
};
const readyScope = Schema.makeFilter(
  (event: {
    environmentId: EnvironmentId;
    task: WorkTaskGraphTaskRef;
    group: WorkTaskManagerGroupRef;
  }) =>
    event.task.environmentId === event.environmentId &&
    event.group.environmentId === event.environmentId,
  { expected: "local fairness task and group" },
);
const readyClaimScope = Schema.makeFilter(
  (event: {
    task: WorkTaskGraphTaskRef;
    group: WorkTaskManagerGroupRef;
    membershipGeneration: WorkTaskMembershipGeneration;
    claim: typeof claimReference.Type;
  }) =>
    sameMembershipTask(event.task, event.claim.binding.task) &&
    sameMembershipGroup(event.group, event.claim.binding.group) &&
    event.membershipGeneration === event.claim.binding.membershipGeneration,
  { expected: "fairness evidence for the original selected task membership" },
);
const readyQualified = Schema.Struct({
  ...readyFields,
  kind: Schema.Literal("qualified"),
  claim: forbidden,
}).check(
  readyScope,
  Schema.makeFilter(
    (event) =>
      event.oldestReadySequence !== null &&
      (event.previousOldestReadySequence === null ||
        event.oldestReadySequence === event.previousOldestReadySequence) &&
      event.basis.taskRevisions.acceptedDefinitionRevision !== null &&
      event.basis.managerGeneration !== null,
    { expected: "qualifying observation preserving existing queue age" },
  ),
);
const readyAccepted = Schema.Struct({
  ...readyFields,
  kind: Schema.Literal("accepted"),
  claim: claimReference,
}).check(
  readyScope,
  readyClaimScope,
  Schema.makeFilter(
    (event) =>
      event.previousOldestReadySequence !== null &&
      event.oldestReadySequence === null &&
      event.sourceEventId === event.claim.acceptedEventId &&
      event.sourceSequence === event.claim.acceptedSequence,
    { expected: "consumed queue age for this accepted claim" },
  ),
);
const readySettled = Schema.Struct({
  ...readyFields,
  kind: Schema.Literal("settled"),
  claim: claimReference,
  releaseEventId: graphEventFields.id,
  releaseSequence: groupCounter,
}).check(
  readyScope,
  readyClaimScope,
  Schema.makeFilter(
    (event) =>
      event.previousOldestReadySequence === null &&
      event.oldestReadySequence !== null &&
      event.claim.acceptedSequence < event.releaseSequence &&
      event.sourceEventId === event.releaseEventId &&
      event.sourceSequence === event.releaseSequence &&
      event.basis.taskRevisions.acceptedDefinitionRevision !== null &&
      event.basis.managerGeneration !== null,
    { expected: "new queue age after original claim release" },
  ),
);
const readyDisqualified = Schema.Struct({
  ...readyFields,
  kind: Schema.Literal("disqualified"),
  claim: forbidden,
  reason: Schema.Literals([
    "task_pause",
    "task_stop",
    "done",
    "archive",
    "prerequisite_changed",
    "definition_changed",
    "policy_changed",
    "membership_changed",
  ]),
}).check(
  readyScope,
  Schema.makeFilter((event) => event.oldestReadySequence === null, {
    expected: "ended task eligibility epoch",
  }),
);
const graphScanPosition = Schema.Struct({
  dependentTaskId: WorkTaskId.check(Schema.isMaxLength(2_000)),
  edgeId: WorkTaskGraphEdgeId,
});
const memberScanPosition = Schema.Struct({ taskId: WorkTaskId.check(Schema.isMaxLength(2_000)) });
const scanFields = {
  ...coordinationFields,
  type: Schema.Literal("task.reconciliation-progressed"),
  completed: Schema.Boolean,
};
const graphScanProgress = Schema.Struct({
  ...scanFields,
  scan: Schema.Struct({
    ...journalSource,
    kind: Schema.Literal("graph_dependents"),
    blocker: WorkTaskGraphTaskRef,
    capturedRevision: WorkTaskGraphRevision,
  }),
  observedRevision: WorkTaskGraphRevision,
  previousPosition: Schema.NullOr(graphScanPosition),
  position: Schema.NullOr(graphScanPosition),
  visited: Schema.Array(WorkTaskGraphTaskRef).check(Schema.isMaxLength(100)),
}).check(
  Schema.makeFilter(
    (event) => {
      const { previousPosition: previous, position, visited, scan } = event;
      if (
        scan.blocker.environmentId !== event.environmentId ||
        event.observedRevision < scan.capturedRevision ||
        new Set(visited.map((row) => row.taskId)).size !== visited.length ||
        visited.some(
          (row) => row.environmentId !== event.environmentId || row.taskId === scan.blocker.taskId,
        ) ||
        previous?.dependentTaskId === scan.blocker.taskId ||
        position?.dependentTaskId === scan.blocker.taskId
      )
        return false;
      const unchanged =
        previous === null
          ? position === null
          : position !== null &&
            previous.dependentTaskId === position.dependentTaskId &&
            previous.edgeId === position.edgeId;
      return visited.length === 0
        ? event.completed && unchanged
        : !unchanged &&
            position !== null &&
            position.dependentTaskId === visited[visited.length - 1]!.taskId;
    },
    { expected: "bounded local non-self graph traversal with a typed position" },
  ),
);
const memberScanProgress = Schema.Struct({
  ...scanFields,
  scan: Schema.Struct({
    ...journalSource,
    kind: Schema.Literal("group_members"),
    group: WorkTaskManagerGroupRef,
    capturedRevision: WorkTaskGroupMembershipRevision,
  }),
  observedRevision: WorkTaskGroupMembershipRevision,
  previousPosition: Schema.NullOr(memberScanPosition),
  position: Schema.NullOr(memberScanPosition),
  visited: Schema.Array(
    Schema.Struct({
      task: WorkTaskGraphTaskRef,
      membershipGeneration: WorkTaskMembershipGeneration,
    }),
  ).check(Schema.isMaxLength(100)),
}).check(
  Schema.makeFilter(
    (event) => {
      const { previousPosition: previous, position, visited, scan } = event;
      if (
        scan.group.environmentId !== event.environmentId ||
        event.observedRevision < scan.capturedRevision ||
        new Set(visited.map((row) => row.task.taskId)).size !== visited.length ||
        visited.some((row) => row.task.environmentId !== event.environmentId)
      )
        return false;
      const unchanged =
        previous === null
          ? position === null
          : position !== null && previous.taskId === position.taskId;
      return visited.length === 0
        ? event.completed && unchanged
        : !unchanged &&
            position !== null &&
            position.taskId === visited[visited.length - 1]!.task.taskId;
    },
    { expected: "bounded local membership traversal with actual generation observations" },
  ),
);
const coordinationEvent = Schema.Union([
  workAccepted,
  workUncertain,
  workReleased,
  dirtyRecorded,
  dirtyAcknowledged,
  readyQualified,
  readyAccepted,
  readySettled,
  readyDisqualified,
  graphScanProgress,
  memberScanProgress,
]);
type KnownJournalReference = {
  id: string;
  sequence?: number | undefined;
  kind?: string | undefined;
  taskId?: string | undefined;
};
const coordinationReferences = (event: typeof coordinationEvent.Type): KnownJournalReference[] => {
  const taskId =
    "claim" in event && event.claim !== undefined
      ? event.claim.binding.task.taskId
      : "task" in event
        ? event.task.taskId
        : undefined;
  const refs: KnownJournalReference[] = [{ id: event.id, kind: event.type, taskId }];
  if ("claim" in event && event.claim !== undefined)
    refs.push({
      id: event.claim.acceptedEventId,
      sequence: event.claim.acceptedSequence,
      kind: "task-group.work-accepted",
      taskId,
    });
  if (event.type === "task-group.work-accepted") {
    refs.push({ id: event.claim.sourceEventId, sequence: event.claim.sourceSequence });
    if (event.claim.launchOrigin.type === "initial_start")
      refs.push({
        id: event.claim.launchOrigin.startEventId,
        sequence: event.claim.launchOrigin.startSequence,
        taskId,
      });
    for (const witness of event.claim.prerequisites)
      refs.push({
        id: witness.completionEventId,
        sequence: witness.completionSequence,
        kind: "task.completed",
        taskId: witness.blocker.taskId,
      });
  } else if (event.type === "task-group.work-released") {
    const evidence = event.releaseEvidence;
    refs.push({
      id: evidence.terminalRunEvent.eventId,
      sequence: evidence.terminalRunEvent.sequence,
      kind: evidence.terminalRunEvent.type,
      taskId,
    });
    if (evidence.type === "run_settled") {
      refs.push({
        id: evidence.attempt.terminalAttemptEvent.eventId,
        sequence: evidence.attempt.terminalAttemptEvent.sequence,
        kind: evidence.attempt.terminalAttemptEvent.type,
        taskId,
      });
      if (evidence.interruptResult !== undefined)
        refs.push({
          id: evidence.interruptResult.eventId,
          sequence: evidence.interruptResult.sequence,
          kind: evidence.interruptResult.type,
          taskId,
        });
    }
  } else if (event.type === "task.reconciliation-progressed")
    refs.push({ id: event.scan.sourceEventId, sequence: event.scan.sourceSequence });
  else {
    refs.push({ id: event.sourceEventId, sequence: event.sourceSequence });
    if (event.type === "task.ready-sequence-updated" && event.kind === "settled")
      refs.push({
        id: event.releaseEventId,
        sequence: event.releaseSequence,
        kind: "task-group.work-released",
        taskId,
      });
  }
  return refs;
};
const compatibleJournalReferences = (refs: readonly KnownJournalReference[]) => {
  const byId = new Map<string, KnownJournalReference>();
  const bySequence = new Map<number, string>();
  for (const ref of refs) {
    const previous = byId.get(ref.id);
    if (
      previous !== undefined &&
      ((previous.sequence !== undefined &&
        ref.sequence !== undefined &&
        previous.sequence !== ref.sequence) ||
        (previous.kind !== undefined && ref.kind !== undefined && previous.kind !== ref.kind) ||
        (previous.taskId !== undefined &&
          ref.taskId !== undefined &&
          previous.taskId !== ref.taskId))
    )
      return false;
    if (ref.sequence !== undefined) {
      const existing = bySequence.get(ref.sequence);
      if (existing !== undefined && existing !== ref.id) return false;
      bySequence.set(ref.sequence, ref.id);
    }
    byId.set(ref.id, {
      id: ref.id,
      sequence: ref.sequence ?? previous?.sequence,
      kind: ref.kind ?? previous?.kind,
      taskId: ref.taskId ?? previous?.taskId,
    });
  }
  return true;
};
export const WorkTaskCoordinationEvent = coordinationEvent.check(
  Schema.makeFilter((event) => compatibleJournalReferences(coordinationReferences(event)), {
    expected: "compatible known journal event identity, sequence, type and task",
  }),
);
export type WorkTaskCoordinationEvent = typeof WorkTaskCoordinationEvent.Type;
export const WorkTaskPersistedCoordinationEvent = Schema.Struct({
  event: WorkTaskCoordinationEvent,
  context: groupEventContext,
}).check(
  exactGroupEventContext,
  Schema.makeFilter(
    ({ event, context }) => {
      const refs = coordinationReferences(event);
      if (
        !compatibleJournalReferences([...refs, { id: event.id, sequence: context.sequence }]) ||
        refs.some((ref) => ref.sequence !== undefined && ref.sequence > context.sequence)
      )
        return false;
      if (event.type === "task-group.work-accepted")
        return event.claim.acceptedSequence === context.sequence;
      if (
        "claim" in event &&
        event.claim !== undefined &&
        event.claim.acceptedSequence >= context.sequence
      )
        return false;
      if (event.type === "task-group.work-released")
        return (
          event.observedSequence < context.sequence &&
          event.releaseEvidence.terminalRunEvent.sequence >= event.claim.acceptedSequence &&
          refs.every(
            (ref) =>
              ref.sequence === undefined ||
              ref.id === event.id ||
              ref.sequence <= event.observedSequence,
          ) &&
          (event.releaseEvidence.type !== "run_settled" ||
            (event.releaseEvidence.attempt.terminalAttemptEvent.sequence >=
              event.claim.acceptedSequence &&
              (event.releaseEvidence.interruptResult === undefined ||
                event.releaseEvidence.interruptResult.sequence >= event.claim.acceptedSequence)))
        );
      const sourceSequence =
        event.type === "task.reconciliation-progressed"
          ? event.scan.sourceSequence
          : event.sourceSequence;
      if (sourceSequence >= context.sequence) return false;
      if (event.type === "task.ready-sequence-updated")
        return (
          (event.previousOldestReadySequence === null ||
            event.previousOldestReadySequence < context.sequence) &&
          (event.oldestReadySequence === null || event.oldestReadySequence <= context.sequence) &&
          ((event.kind !== "qualified" && event.kind !== "settled") ||
            event.previousOldestReadySequence !== null ||
            event.oldestReadySequence === context.sequence)
        );
      return true;
    },
    { expected: "committed coordination context with ordered causal and fairness evidence" },
  ),
);
export type WorkTaskPersistedCoordinationEvent = typeof WorkTaskPersistedCoordinationEvent.Type;
