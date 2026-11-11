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
