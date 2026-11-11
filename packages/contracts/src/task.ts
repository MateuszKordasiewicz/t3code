import * as Schema from "effect/Schema";

import { PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";

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
  code: Schema.Literals(["not_found", "invalid_definition", "revision_conflict", "invalid_state"]),
  message: Schema.String,
  taskId: Schema.optional(WorkTaskId),
}) {}
