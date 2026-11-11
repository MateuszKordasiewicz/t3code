import * as Schema from "effect/Schema";

import {
  AuthSessionId,
  CommandId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ProviderSessionId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

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

export const WorkTaskResultReference = Schema.Union([
  Schema.Struct({ type: Schema.Literal("run"), threadId: ThreadId, runId: RunId }),
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
