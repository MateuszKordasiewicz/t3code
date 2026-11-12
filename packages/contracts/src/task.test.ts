import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerConfig } from "./server.ts";
import * as task from "./task.ts";
import { OrchestrationV2Run, OrchestrationV2RunJson } from "./orchestrationV2.ts";

import {
  WorkTaskAcceptedDefinition,
  WorkTaskActionPolicy,
  WorkTaskDraftDefinition,
  WorkTaskError,
  WorkTaskAdoptDefinitionInput,
  WorkTaskAdoptDefinitionResult,
  WorkTaskCallerContext,
  WorkTaskCreateDraftInput,
  WorkTaskCreateDraftResult,
  WorkTaskLinkThreadInput,
  WorkTaskLinkThreadResult,
  WorkTaskProposeDefinitionInput,
  WorkTaskProposeDefinitionResult,
  WorkTaskUnlinkThreadInput,
  WorkTaskUnlinkThreadResult,
} from "./task.ts";

const criterion = { id: "criterion-1", condition: "Works", verification: "Run tests" };
const definition = {
  revision: 1,
  title: "Task",
  objective: "Finish work",
  criteria: [criterion],
  actionPolicy: { version: 1, rules: [] },
};
const decodeDraft = Schema.decodeUnknownSync(WorkTaskDraftDefinition);
const decodeAccepted = Schema.decodeUnknownSync(WorkTaskAcceptedDefinition);
const decodeCreateDraft = Schema.decodeUnknownSync(WorkTaskCreateDraftInput);
const decodeDetailInput = Schema.decodeUnknownSync(task.WorkTaskDetailInput);
const decodeThreadLink = Schema.decodeUnknownSync(task.WorkTaskThreadLink);
const decodeProposalResult = Schema.decodeUnknownSync(WorkTaskProposeDefinitionResult);
const decodeAdoptDefinition = Schema.decodeUnknownSync(WorkTaskAdoptDefinitionInput);
const decodeResume = Schema.decodeUnknownSync(task.WorkTaskResumeInput);
const decodeManagerRunBinding = Schema.decodeUnknownSync(task.WorkTaskManagerRunBinding);
const decodeReceipt = Schema.decodeUnknownSync(task.WorkTaskReceipt);

describe("staged task events", () => {
  const revisions = {
    definitionRevision: 1,
    lifecycleRevision: 2,
    completionPolicyRevision: 3,
    actionPolicyRevision: 4,
  };
  const base = {
    id: "event",
    taskId: "task",
    occurredAt: "2026-11-11T00:00:00.000Z",
    attribution: { type: "server" },
    ...revisions,
  };
  const identity = { action: "git.push", target: "origin/main", fingerprint: "sha256:123" };
  const decode = Schema.decodeUnknownSync(task.WorkTaskEvent);

  it("retains independent revisions and a manager-only run binding", () => {
    const binding = { taskId: "task", managerGeneration: 5, ...revisions };
    expect(decodeManagerRunBinding(binding)).toEqual(binding);
    expect(OrchestrationV2Run.fields.taskManagerBinding).toBeDefined();
    expect(OrchestrationV2RunJson.fields.taskManagerBinding).toBeDefined();
    expect(() =>
      decodeManagerRunBinding({
        ...binding,
        managerGeneration: 0,
      }),
    ).toThrow();
    const receipt = {
      commandId: "command",
      taskId: "task",
      commandType: "task.start",
      sequence: 5,
      managerGeneration: 5,
      ...revisions,
    };
    expect(decodeReceipt(receipt)).toEqual(receipt);
    expect(() => decodeReceipt({ ...receipt, completionPolicyRevision: 0 })).toThrow();
    for (const type of ["task.created", "task.definition-updated"]) {
      const event = { ...base, type, definition };
      expect(decode(event)).toBeDefined();
      expect(() => decode({ ...event, definition: { ...definition, revision: 99 } })).toThrow();
      expect(() => decode({ ...event, definitionRevision: 2 })).toThrow();
      expect(
        decode({ ...event, definitionRevision: 2, definition: { ...definition, revision: 2 } }),
      ).toBeDefined();
    }
  });

  it("records explicit human acceptance of a complete definition with persisted identity", () => {
    const accepted = {
      ...base,
      type: "task.definition-accepted",
      attribution: { type: "user", subject: "human", sessionId: "session" },
      definition,
    };
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = {
      sequence: 2,
      eventId: "event",
      occurredAt: base.occurredAt,
      commandId: "command",
    };
    expect(persisted({ event: accepted, context })).toMatchObject({ event: accepted, context });
    for (const type of ["task.created", "task.definition-updated"]) {
      const draft = { ...accepted, type, definition: { ...definition, objective: "" } };
      expect(decode(draft).type).toBe(type);
      expect(decode(draft).type).not.toBe("task.definition-accepted");
    }
    for (const change of [
      { definition: { ...definition, title: "" } },
      { definition: { ...definition, objective: " " } },
      { definition: { ...definition, criteria: [] } },
      { definition: { ...definition, criteria: [{ ...criterion, verification: "" }] } },
      { definition: { ...definition, revision: 2 } },
      { definitionRevision: 0 },
      { actionPolicyRevision: 0 },
      { id: "" },
      { id: "x".repeat(2_001) },
      { taskId: "x".repeat(2_001) },
      { attribution: { type: "server" } },
      { attribution: { type: "user", subject: "human" } },
      { attribution: { type: "user", subject: "x".repeat(2_001), sessionId: "session" } },
    ])
      expect(() => decode({ ...accepted, ...change })).toThrow();
    for (const change of [
      { eventId: "other" },
      { commandId: null },
      { commandId: "x".repeat(2_001) },
    ])
      expect(() => persisted({ event: accepted, context: { ...context, ...change } })).toThrow();
    expect(decode({ ...accepted, id: "e".repeat(2_000), taskId: "t".repeat(2_000) })).toBeDefined();
  });

  it("preserves result attribution and an optional actual contributor thread", () => {
    const result = {
      ...base,
      type: "task.result-recorded",
      sourceId: "source",
      summary: "Result",
      results: [{ type: "artifact", uri: "artifact", hash: "hash" }],
    };
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = { sequence: 3, eventId: "event", occurredAt: base.occurredAt, commandId: null };
    expect(persisted({ event: result, context }).event).toEqual(result);
    expect(decode({ ...result, contributorThreadId: "thread" })).toMatchObject({
      contributorThreadId: "thread",
    });
    for (const contributorThreadId of ["", "x".repeat(2_001)])
      expect(() => decode({ ...result, contributorThreadId })).toThrow();
    expect(() => decode({ ...result, attribution: undefined })).toThrow();
    expect(() =>
      decode({
        ...result,
        attribution: { type: "user", subject: "human", sessionId: "x".repeat(2_001) },
      }),
    ).toThrow();
    expect(() => decode({ ...result, results: [] })).toThrow();
    expect(() =>
      decode({ ...result, results: Array.from({ length: 33 }, () => result.results[0]) }),
    ).toThrow();
  });

  it("records terminal review evidence and purpose-bound continuations in persisted context", () => {
    const binding = {
      taskId: "task",
      reviewId: "review",
      reviewRevision: 1,
      definitionRevision: 1,
      actionPolicyRevision: 4,
      payloadIdentity: identity,
      ruleId: "rule",
      runtimeRequestId: "runtime",
      providerSessionId: "session",
      managerThreadId: "manager",
      managerGeneration: 5,
      runId: "run",
    };
    const source = {
      type: "task.archive",
      commandId: "archive-command",
      human: { subject: "human", sessionId: "session" },
    };
    const invalidated = {
      ...base,
      type: "task.action-review-invalidated",
      cause: "policy_changed",
      binding,
      reason: "policy changed",
    };
    const cancelled = {
      ...base,
      type: "task.action-review-cancelled",
      binding,
      reason: "archived",
      source,
    };
    const release = {
      ...base,
      type: "task.action-review-continuation-released",
      binding,
      continuationId: "continuation",
      reason: "human decision",
      decisionEventId: "decision",
      purpose: "approved_action",
      decision: "approve",
    };
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = { sequence: 2, eventId: "event", occurredAt: base.occurredAt, commandId: null };
    for (const event of [invalidated, cancelled, release]) {
      expect(decode(event)).toBeDefined();
      expect(
        persisted({
          event,
          context: { ...context, commandId: event === cancelled ? source.commandId : null },
        }),
      ).toBeDefined();
      for (const field of [
        "taskId",
        "reviewId",
        "reviewRevision",
        "definitionRevision",
        "actionPolicyRevision",
        "payloadIdentity",
        "ruleId",
        "runtimeRequestId",
        "providerSessionId",
        "managerThreadId",
        "managerGeneration",
        "runId",
      ]) {
        expect(() => decode({ ...event, binding: { ...binding, [field]: undefined } })).toThrow();
      }
      const matchingFields =
        event === invalidated
          ? ["taskId"]
          : ["taskId", "definitionRevision", "actionPolicyRevision"];
      for (const field of matchingFields) {
        expect(() =>
          decode({ ...event, binding: { ...binding, [field]: field === "taskId" ? "other" : 99 } }),
        ).toThrow();
      }
      expect(() => persisted({ event, context: { ...context, eventId: "other" } })).toThrow();
      expect(() =>
        persisted({ event, context: { ...context, occurredAt: "2026-11-12T00:00:00Z" } }),
      ).toThrow();
      expect(() => decode({ ...event, attribution: { type: "user", subject: "human" } })).toThrow();
    }
    for (const revisions of [
      { actionPolicyRevision: 5 },
      { definitionRevision: 2 },
      { definitionRevision: 2, actionPolicyRevision: 5 },
    ]) {
      const event = { ...invalidated, ...revisions };
      expect(decode(event)).toMatchObject({ binding });
      expect(persisted({ event, context })).toMatchObject({ event: { binding } });
    }
    for (const cause of ["stop", "pause", "timeout", "dismissal"])
      expect(() => decode({ ...invalidated, cause })).toThrow();
    for (const key of ["action", "target", "fingerprint"])
      expect(() =>
        decode({
          ...release,
          binding: { ...binding, payloadIdentity: { ...identity, [key]: " " } },
        }),
      ).toThrow();
    for (const decision of ["reject", "request_changes"]) {
      const feedback = { ...release, purpose: "feedback", decision };
      expect(decode({ ...feedback, actionAuthority: "grant" })).toEqual(feedback);
      expect(() => decode({ ...feedback, purpose: "approved_action" })).toThrow();
    }
    expect(() => decode({ ...release, purpose: "feedback" })).toThrow();
    for (const field of ["continuationId", "decisionEventId"]) {
      for (const value of [undefined, " ", "x".repeat(2_001)])
        expect(() => decode({ ...release, [field]: value })).toThrow();
      expect(decode({ ...release, [field]: "x".repeat(2_000) })).toBeDefined();
    }
    for (const event of [invalidated, cancelled, release]) {
      for (const reason of [undefined, " ", "x".repeat(2_001)])
        expect(() => decode({ ...event, reason })).toThrow();
      expect(decode({ ...event, reason: "x".repeat(2_000) })).toBeDefined();
    }
    for (const badSource of [
      undefined,
      { ...source, type: "task.stop" },
      { ...source, human: undefined },
      { ...source, commandId: " " },
    ])
      expect(() => decode({ ...cancelled, source: badSource })).toThrow();
    for (const commandId of [null, "other", undefined])
      expect(() => persisted({ event: cancelled, context: { ...context, commandId } })).toThrow();
  });

  it("keeps server-recorded human action decisions distinct from validation", () => {
    const request = {
      ...base,
      type: "task.action-review-requested",
      review: {
        taskId: "task",
        reviewId: "review",
        revision: 1,
        managerThreadId: "manager",
        managerGeneration: 5,
        providerSessionId: "session",
        runId: "run",
        ruleId: "rule",
        definitionRevision: 1,
        actionPolicyRevision: 4,
        ...identity,
        label: "Push",
        preview: { content: "Push changes", artifacts: [] },
        state: { type: "pending" },
        createdSequence: 1,
        createdAt: "2026-11-11T00:00:00Z",
      },
    };
    expect(decode(request)).toBeDefined();
    const decodePersisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = {
      sequence: 1,
      eventId: "event",
      occurredAt: base.occurredAt,
      commandId: "command",
    };
    const published = {
      event: { ...request, review: { ...request.review, runtimeRequestId: "runtime" } },
      context,
    };
    expect(decodePersisted(published)).toBeDefined();
    const preciseTime = "2026-11-11T00:00:00.123456Z";
    const precise = {
      event: {
        ...published.event,
        occurredAt: preciseTime,
        review: { ...published.event.review, createdAt: preciseTime },
      },
      context: { ...context, occurredAt: preciseTime },
    };
    expect(decodePersisted(precise)).toBeDefined();
    for (const length of [2_000, 2_001]) {
      const timestamp = `2026-11-11T00:00:00.${"0".repeat(length - 21)}Z`;
      const bounded = {
        event: {
          ...published.event,
          occurredAt: timestamp,
          review: { ...published.event.review, createdAt: timestamp },
        },
        context: { ...context, occurredAt: timestamp },
      };
      if (length === 2_000) expect(decodePersisted(bounded)).toBeDefined();
      else expect(() => decodePersisted(bounded)).toThrow();
    }

    expect(() =>
      decodePersisted({
        ...precise,
        event: {
          ...precise.event,
          review: { ...precise.event.review, createdAt: "2026-11-11T00:00:00.123457Z" },
        },
      }),
    ).toThrow();
    const ordinary = {
      event: { ...base, type: "task.reopened" },
      context: { ...context, commandId: null },
    };
    expect(decodePersisted(ordinary)).toBeDefined();
    for (const invalidContext of [
      undefined,
      { ...context, sequence: undefined },
      { ...context, sequence: -1 },
      { ...context, sequence: 1.5 },
      { ...context, eventId: undefined },
      { ...context, eventId: "other" },
      { ...context, occurredAt: undefined },
      { ...context, occurredAt: "invalid" },
      { ...context, occurredAt: "2026-02-30T00:00:00Z" },
      { ...context, occurredAt: "2026-11-12T00:00:00.000Z" },
      { ...context, commandId: undefined },
      { ...context, commandId: null },
      { ...context, commandId: " " },
      { ...context, commandId: "x".repeat(2_001) },
    ])
      expect(() => decodePersisted({ ...published, context: invalidContext })).toThrow();
    for (const review of [
      { ...published.event.review, runtimeRequestId: undefined },
      { ...published.event.review, runtimeRequestId: " " },
      { ...published.event.review, runtimeRequestId: "x".repeat(2_001) },
      { ...published.event.review, taskId: "other" },
      { ...published.event.review, definitionRevision: 2 },
      { ...published.event.review, actionPolicyRevision: 5 },
      { ...published.event.review, managerGeneration: 0 },
      { ...published.event.review, fingerprint: " " },
      { ...published.event.review, createdSequence: 2 },
      { ...published.event.review, createdAt: "invalid" },
      { ...published.event.review, createdAt: "2026-11-12T00:00:00Z" },
    ])
      expect(() =>
        decodePersisted({ ...published, event: { ...published.event, review } }),
      ).toThrow();
    expect(
      decodePersisted({
        ...published,
        event: {
          ...published.event,
          review: { ...published.event.review, runtimeRequestId: "r".repeat(2_000) },
        },
      }),
    ).toBeDefined();
    expect(() =>
      decodePersisted({ ...ordinary, context: { ...ordinary.context, commandId: undefined } }),
    ).toThrow();
    expect(() =>
      decodePersisted({
        ...ordinary,
        event: { ...ordinary.event, occurredAt: "invalid" },
        context: { ...ordinary.context, occurredAt: "invalid" },
      }),
    ).toThrow();
    expect(() => decode({ ...request, taskId: "other" })).toThrow();
    expect(() => decode({ ...request, actionPolicyRevision: 5 })).toThrow();
    expect(() =>
      decode({ ...request, review: { ...request.review, state: { type: "approved" } } }),
    ).toThrow();
    const result = {
      ...base,
      type: "task.action-review-result-recorded",
      reviewId: "review",
      reviewRevision: 1,
      managerThreadId: "manager",
      managerGeneration: 5,
      runId: "run",
      payloadIdentity: identity,
      decision: "approve",
      human: { subject: "human", sessionId: "session" },
    };
    expect(decode(result)).toBeDefined();
    for (const key of ["managerThreadId", "runId"] as const) {
      expect(decode({ ...result, [key]: "x".repeat(2_000) })).toBeDefined();
      expect(() => decode({ ...result, [key]: "x".repeat(2_001) })).toThrow();
    }
    expect(
      decode({ ...result, human: { ...result.human, sessionId: "x".repeat(2_000) } }),
    ).toBeDefined();
    expect(() =>
      decode({ ...result, human: { ...result.human, sessionId: "x".repeat(2_001) } }),
    ).toThrow();
    for (const invalid of [
      {
        ...result,
        attribution: {
          type: "agent",
          threadId: "manager",
          providerSessionId: "session",
          runId: "run",
        },
      },
      { ...result, human: undefined },
      { ...result, payloadIdentity: { ...identity, fingerprint: "" } },
      { ...result, actionPolicyRevision: 0 },
      { ...result, decision: "pass" },
    ])
      expect(() => decode(invalid)).toThrow();
    expect(() => decode({ ...result, type: "task.validation-recorded" })).toThrow();
  });
});

describe("work task definitions", () => {
  it("allows incomplete drafts but requires executable accepted definitions", () => {
    expect(decodeDraft({ ...definition, title: "", objective: "", criteria: [] })).toBeDefined();
    expect(decodeAccepted(definition)).toBeDefined();
    for (const incomplete of [
      { title: " " },
      { objective: "" },
      { criteria: [] },
      { criteria: [{ ...criterion, condition: " " }] },
      { criteria: [{ ...criterion, verification: "" }] },
    ]) {
      expect(() => decodeAccepted({ ...definition, ...incomplete })).toThrow();
    }
  });

  it("enforces text, count, identity and revision bounds", () => {
    expect(
      decodeDraft({
        ...definition,
        title: "x".repeat(240),
        objective: "x".repeat(12_000),
        data: "x".repeat(16_000),
        criteria: Array.from({ length: 64 }, (_, i) => ({
          id: String(i),
          condition: "x".repeat(2_000),
          verification: "x".repeat(2_000),
        })),
      }),
    ).toBeDefined();
    for (const invalid of [
      { title: "x".repeat(241) },
      { objective: "x".repeat(12_001) },
      { data: "x".repeat(16_001) },
      { criteria: [{ ...criterion, condition: "x".repeat(2_001) }] },
      { criteria: [{ ...criterion, verification: "x".repeat(2_001) }] },
      { criteria: Array.from({ length: 65 }, (_, i) => ({ ...criterion, id: String(i) })) },
      { criteria: [criterion, criterion] },
      { revision: 0 },
    ]) {
      expect(() => decodeDraft({ ...definition, ...invalid })).toThrow();
    }
  });

  it("accepts only versioned, uniquely identified exact action rules", () => {
    const decode = Schema.decodeUnknownSync(WorkTaskActionPolicy);
    const rule = {
      id: "rule-1",
      action: "git.push",
      target: "origin/main",
      decision: "review-first",
    };
    expect(decode({ version: 1, rules: [rule] })).toBeDefined();
    const rules = Array.from({ length: 32 }, (_, i) => ({ ...rule, id: String(i) }));
    expect(decode({ version: 1, rules })).toBeDefined();
    expect(
      decode({
        version: 1,
        rules: [{ ...rule, action: "x".repeat(120), target: "x".repeat(2_000) }],
      }),
    ).toBeDefined();
    for (const policy of [
      { version: 1, rules: [...rules, { ...rule, id: "33" }] },
      { version: 1, rules: [{ ...rule, action: "x".repeat(121) }] },
      { version: 1, rules: [{ ...rule, target: "x".repeat(2_001) }] },
      { version: 2, rules: [] },
      { version: 1, rules: [rule, rule] },
      { version: 1, rules: [{ ...rule, target: "*" }] },
      { version: 1, rules: [{ ...rule, action: " " }] },
      { version: 1, rules: [{ ...rule, decision: "agent-review" }] },
    ])
      expect(() => decode(policy)).toThrow();
  });

  it("decodes typed errors", () => {
    const decode = Schema.decodeUnknownSync(WorkTaskError);
    expect(
      decode({
        _tag: "WorkTaskError",
        code: "revision_conflict",
        message: "stale",
        taskId: "task-1",
      }).code,
    ).toBe("revision_conflict");
    for (const code of [
      "feature_disabled",
      "unsupported",
      "unavailable",
      "invalid_page_token",
      "stale_page_token",
    ]) {
      expect(
        decode({ _tag: "WorkTaskError", code, message: "unavailable", snapshot: definition }),
      ).not.toHaveProperty("snapshot");
    }
    expect(() => decode({ _tag: "WorkTaskError", code: "other", message: "bad" })).toThrow();
  });
});

describe("work task API", () => {
  it("decodes absent, disabled and enabled capability", () => {
    const decode = Schema.decodeUnknownSync(
      Schema.Struct({ workTasks: ServerConfig.fields.workTasks }),
    );
    expect(decode({}).workTasks).toBeUndefined();
    expect(decode({ workTasks: false }).workTasks).toBe(false);
    expect(decode({ workTasks: true }).workTasks).toBe(true);
  });

  it("saves incomplete drafts and keeps server-derived identity out of inputs", () => {
    const content = { ...definition, revision: undefined, title: "", objective: "", criteria: [] };
    const payload = {
      taskId: "t",
      commandId: "c",
      definition: content,
      actor: { type: "user" },
      caller: { type: "server" },
    };
    const decoded = decodeCreateDraft(payload);
    expect(decoded).toEqual({
      taskId: "t",
      commandId: "c",
      definition: { title: "", objective: "", criteria: [], actionPolicy: definition.actionPolicy },
    });
    const caller = Schema.decodeUnknownSync(WorkTaskCallerContext);
    expect(caller({ type: "user", sessionId: "session", subject: "person" }).type).toBe("user");
    expect(
      caller({
        type: "manager",
        threadId: "thread",
        providerSessionId: "session",
        runId: "run",
        generation: 1,
      }).type,
    ).toBe("manager");
    expect(() => caller({ type: "manager", threadId: "thread", generation: 1 })).toThrow();
  });

  it("requires the appropriate revision domains for proposals, adoption and links", () => {
    const content = {
      title: "",
      objective: "",
      criteria: [],
      actionPolicy: definition.actionPolicy,
    };
    const proposal = {
      taskId: "t",
      commandId: "c",
      expectedDefinitionRevision: 1,
      expectedActionPolicyRevision: 2,
      definition: content,
    };
    const propose = Schema.decodeUnknownSync(WorkTaskProposeDefinitionInput);
    const adopt = Schema.decodeUnknownSync(WorkTaskAdoptDefinitionInput);
    expect(propose(proposal)).toEqual(proposal);
    expect(() => adopt(proposal)).toThrow();
    expect(adopt({ ...proposal, expectedLifecycleRevision: 3 }).expectedLifecycleRevision).toBe(3);
    for (const key of [
      "expectedDefinitionRevision",
      "expectedActionPolicyRevision",
      "expectedLifecycleRevision",
    ] as const) {
      expect(() =>
        adopt({ ...proposal, expectedLifecycleRevision: 3, [key]: undefined }),
      ).toThrow();
    }
    for (const key of ["expectedDefinitionRevision", "expectedActionPolicyRevision"] as const) {
      expect(() => propose({ ...proposal, [key]: undefined })).toThrow();
    }
    for (const schema of [WorkTaskLinkThreadInput, WorkTaskUnlinkThreadInput]) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(
        decode({
          taskId: "t",
          commandId: "c",
          threadId: "thread",
          expectedLifecycleRevision: 1,
          actor: { type: "user" },
        }),
      ).toEqual({ taskId: "t", commandId: "c", threadId: "thread", expectedLifecycleRevision: 1 });
      expect(() => decode({ taskId: "t", commandId: "c", threadId: "thread" })).toThrow();
    }
  });

  it("returns committed revision domains and sequence for each operation", () => {
    const result = {
      taskId: "t",
      definitionRevision: 1,
      lifecycleRevision: 2,
      actionPolicyRevision: 3,
      sequence: 4,
    };
    for (const schema of [
      WorkTaskCreateDraftResult,
      WorkTaskAdoptDefinitionResult,
      WorkTaskLinkThreadResult,
      WorkTaskUnlinkThreadResult,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(decode(result)).toEqual(result);
      expect(() => decode({ ...result, lifecycleRevision: undefined })).toThrow();
    }
    expect(
      decodeProposalResult({
        ...result,
        proposal: { title: "", objective: "", criteria: [], actionPolicy: definition.actionPolicy },
      }).sequence,
    ).toBe(4);
  });
});

describe("bounded task reads", () => {
  const summary = {
    taskId: "task",
    title: "Task",
    definitionRevision: 1,
    lifecycleRevision: 1,
    desiredMode: "active",
    archived: false,
    execution: { state: "waiting" },
    attention: { state: "none" },
    completion: "incomplete",
  };
  const reference = { type: "run", threadId: "thread", runId: "run" };
  const contribution = {
    id: "result",
    threadId: "thread",
    attribution: { type: "server" },
    sequence: 3,
    summary: "Result",
    results: [reference],
  };

  it("bounds page requests and requires snapshot sequences on all read results", () => {
    for (const schema of [
      task.WorkTaskListInput,
      task.WorkTaskThreadLinksInput,
      task.WorkTaskHistoryInput,
      task.WorkTaskMembershipsInput,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      const input = {
        taskId: "task",
        threadId: "thread",
        limit: 100,
        pageToken: "x".repeat(2_000),
      };
      expect(decode(input)).toBeDefined();
      expect(decode({ ...input, limit: undefined }).limit).toBeUndefined();
      for (const invalid of [
        { limit: 0 },
        { limit: 101 },
        { limit: 1.5 },
        { pageToken: "" },
        { pageToken: "x".repeat(2_001) },
      ]) {
        expect(() => decode({ ...input, ...invalid })).toThrow();
      }
    }
    const pages = [
      [task.WorkTaskListResult, "tasks", summary],
      [task.WorkTaskThreadLinksResult, "links", { threadId: "thread", role: "contributor" }],
      [task.WorkTaskHistoryResult, "contributions", contribution],
      [task.WorkTaskMembershipsResult, "tasks", summary],
    ] as const;
    for (const [schema, key, item] of pages) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(
        decode({ sequence: 0, [key]: Array(100).fill(item), nextPageToken: "next" }),
      ).toBeDefined();
      expect(() => decode({ sequence: 0, [key]: Array(101).fill(item) })).toThrow();
      expect(() => decode({ [key]: [] })).toThrow();
      expect(() => decode({ sequence: -1, [key]: [] })).toThrow();
      expect(() => decode({ sequence: 0, [key]: [], nextPageToken: "x".repeat(2_001) })).toThrow();
    }
    const detail = Schema.decodeUnknownSync(task.WorkTaskDetailResult);
    const detailState = {
      sequence: 1,
      summary,
      definition,
      actionPolicyRevision: 2,
      completionPolicyRevision: 3,
      allowanceOverride: null,
      completionPolicy: { type: "inherit" },
      consumedAllowance: 0,
    };
    const decoded = detail(detailState);
    expect(decoded.actionPolicyRevision).toBe(2);
    expect(
      decodeAdoptDefinition({
        taskId: decoded.summary.taskId,
        commandId: "adopt",
        expectedDefinitionRevision: decoded.summary.definitionRevision,
        expectedLifecycleRevision: decoded.summary.lifecycleRevision,
        expectedActionPolicyRevision: decoded.actionPolicyRevision,
        definition: decoded.definition,
      }),
    ).toBeDefined();
    for (const actionPolicyRevision of [undefined, 0]) {
      expect(() => detail({ ...detailState, actionPolicyRevision })).toThrow();
    }
    expect(() => detail({ ...detailState, sequence: undefined })).toThrow();
    expect(() => decodeDetailInput({})).toThrow();
  });

  it("separates desired mode, execution, attention and completion and bounds text", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskSummary);
    expect(
      decode({
        ...summary,
        desiredMode: "paused",
        archived: false,
        execution: { state: "running" },
        attention: { state: "needed", reason: "x".repeat(2_000) },
        completion: "ready_for_review",
      }),
    ).toBeDefined();
    for (const invalid of [
      { desiredMode: "running" },
      { execution: { state: "active" } },
      { attention: { state: "needed" } },
      { attention: { state: "needed", reason: "x".repeat(2_001) } },
      { completion: "approved" },
      { title: "x".repeat(241) },
      { definitionRevision: 0 },
    ])
      expect(() => decode({ ...summary, ...invalid })).toThrow();
    expect(() => decodeThreadLink({ threadId: "thread", role: "owner" })).toThrow();
  });

  it("bounds contribution batches and validates concrete reference variants", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskContribution);
    const { threadId: _threadId, ...unattributed } = contribution;
    expect(decode(unattributed)).toEqual(unattributed);
    expect(() => decode({ ...contribution, attribution: undefined })).toThrow();
    expect(() =>
      decode({
        ...contribution,
        attribution: {
          type: "agent",
          threadId: "thread",
          providerSessionId: "session",
          runId: "x".repeat(2_001),
        },
      }),
    ).toThrow();
    expect(() => decode({ ...contribution, threadId: "x".repeat(2_001) })).toThrow();
    const references = [
      reference,
      { type: "artifact", uri: "file", hash: "sha256:abc" },
      { type: "source_revision", repository: "repo", revision: "abc" },
    ];
    expect(
      decode({
        ...contribution,
        summary: "x".repeat(2_000),
        results: Array.from({ length: 32 }, () => ({ ...reference })),
      }),
    ).toBeDefined();
    expect(decode({ ...contribution, results: references })).toBeDefined();
    for (const invalid of [
      { results: Array.from({ length: 33 }, () => ({ ...reference })) },
      { results: [{ type: "run", runId: "run" }] },
      { results: [{ type: "artifact", uri: "file" }] },
      { results: [{ type: "source_revision", repository: "repo" }] },
      { results: [{ type: "unknown" }] },
      { summary: "x".repeat(2_001) },
      { id: "" },
      { results: [{ type: "artifact", uri: "x".repeat(2_001), hash: "hash" }] },
    ])
      expect(() => decode({ ...contribution, ...invalid })).toThrow();
  });

  it("requires typed invalidations or an explicit resync reason and bounded cursor", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskSubscriptionEvent);
    const invalidation = { taskId: "task", revision: 1, sequence: 1, change: "membership" };
    for (const change of [
      "definition",
      "lifecycle",
      "execution",
      "membership",
      "contribution",
      "completion",
      "deleted",
    ]) {
      expect(
        decode({
          type: "invalidation",
          invalidation: { ...invalidation, change },
          cursor: "x".repeat(2_000),
        }),
      ).toBeDefined();
    }
    for (const reason of ["cursor-expired", "buffer-overflow", "replay-unavailable"]) {
      expect(decode({ type: "resync-required", reason, sequence: 1 })).toBeDefined();
    }
    for (const invalid of [
      { type: "invalidation", invalidation, cursor: "" },
      { type: "invalidation", invalidation, cursor: "x".repeat(2_001) },
      { type: "invalidation", invalidation: { ...invalidation, change: "other" }, cursor: "x" },
      { type: "invalidation", invalidation: { ...invalidation, revision: 0 }, cursor: "x" },
      { type: "resync-required", reason: "other", sequence: 1 },
      { type: "resync-required", reason: "cursor-expired" },
      { type: "other" },
    ])
      expect(() => decode(invalid)).toThrow();
    const subscribe = Schema.decodeUnknownSync(task.WorkTaskSubscribeInput);
    expect(subscribe({})).toEqual({});
    expect(() => subscribe({ cursor: "x".repeat(2_001) })).toThrow();
  });
});

describe("task configuration and lifecycle", () => {
  const guards = {
    taskId: "task",
    commandId: "command",
    expectedDefinitionRevision: 1,
    expectedLifecycleRevision: 2,
    expectedActionPolicyRevision: 3,
    expectedCompletionPolicyRevision: 4,
    expectedManagerGeneration: null,
  };
  const selection = {
    projectId: "project",
    modelSelection: { instanceId: "codex", model: "model" },
    workspace: { type: "root" },
  };

  it("requires finite positive whole allowance and explicit completion configuration", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskConfigureInput);
    const config = { ...guards, allowanceOverride: 5, completionPolicy: { type: "inherit" } };
    expect(decode(config).allowanceOverride).toBe(5);
    expect(
      decode({
        ...config,
        allowanceOverride: null,
        completionPolicy: {
          type: "configured",
          policy: { type: "manual" },
        },
      }).allowanceOverride,
    ).toBeNull();
    expect(
      decode({
        ...config,
        completionPolicy: {
          type: "configured",
          policy: { type: "automatic", validator: selection },
        },
      }),
    ).toBeDefined();
    for (const allowanceOverride of [0, -1, 1.5, Infinity, NaN]) {
      expect(() => decode({ ...config, allowanceOverride })).toThrow();
    }
    for (const completionPolicy of [
      { type: "automatic" },
      { type: "configured", policy: { type: "automatic" } },
      { type: "configured", policy: { type: "other" } },
    ])
      expect(() => decode({ ...config, completionPolicy })).toThrow();
    const topUp = Schema.decodeUnknownSync(task.WorkTaskTopUpAllowanceInput);
    expect(topUp({ ...guards, newAllowanceCap: 2, consumedAllowance: 1, actor: "user" })).toEqual({
      taskId: "task",
      commandId: "command",
      expectedLifecycleRevision: 2,
      expectedManagerGeneration: null,
      newAllowanceCap: 2,
    });
    for (const allowance of [0, -1, 1.5, Infinity, NaN]) {
      expect(() => topUp({ ...guards, newAllowanceCap: allowance })).toThrow();
    }
  });

  it("requires explicit project, provider, model and workspace for manager and validator", () => {
    const start = Schema.decodeUnknownSync(task.WorkTaskStartInput);
    const replace = Schema.decodeUnknownSync(task.WorkTaskReplaceManagerInput);
    const policy = Schema.decodeUnknownSync(task.WorkTaskCompletionPolicy);
    for (const manager of [
      { ...selection, projectId: undefined },
      { ...selection, modelSelection: { model: "model" } },
      { ...selection, modelSelection: { instanceId: "codex" } },
      { ...selection, workspace: undefined },
      { ...selection, workspace: { type: "worktree" } },
    ]) {
      expect(() => start({ ...guards, manager })).toThrow();
      expect(() => replace({ ...guards, manager })).toThrow();
      expect(() => policy({ type: "automatic", validator: manager })).toThrow();
    }
    expect(start({ ...guards, manager: selection })).toBeDefined();
    expect(replace({ ...guards, manager: selection })).toBeDefined();
    expect(policy({ type: "automatic", validator: selection })).toBeDefined();
  });

  it("requires recoverable guards on lifecycle and reverse actions, without payload authority", () => {
    for (const schema of [
      task.WorkTaskPauseInput,
      task.WorkTaskArchiveInput,
      task.WorkTaskUnarchiveInput,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      const decoded = decode({ ...guards, caller: { type: "user" } });
      expect(decoded).not.toHaveProperty("caller");
      for (const key of ["expectedLifecycleRevision", "expectedManagerGeneration"]) {
        expect(() => decode({ ...guards, [key]: undefined })).toThrow();
      }
    }
    for (const schema of [
      task.WorkTaskResumeInput,
      task.WorkTaskReopenInput,
      task.WorkTaskCompleteManuallyInput,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(decode(guards)).toEqual(guards);
      for (const key of [
        "expectedDefinitionRevision",
        "expectedLifecycleRevision",
        "expectedActionPolicyRevision",
        "expectedCompletionPolicyRevision",
        "expectedManagerGeneration",
      ]) {
        expect(() => decode({ ...guards, [key]: undefined })).toThrow();
      }
    }
    const detail = Schema.decodeUnknownSync(task.WorkTaskDetailResult);
    const snapshot = {
      sequence: 1,
      summary: {
        taskId: "task",
        title: "Task",
        definitionRevision: 1,
        lifecycleRevision: 2,
        desiredMode: "paused",
        archived: false,
        execution: { state: "idle", managerGeneration: 5 },
        attention: { state: "none" },
        completion: "incomplete",
      },
      definition,
      actionPolicyRevision: 3,
      completionPolicyRevision: 4,
      allowanceOverride: null,
      completionPolicy: { type: "inherit" },
      consumedAllowance: 2,
      resolvedAllowance: 5,
    };
    const read = detail(snapshot);
    expect(
      decodeResume({
        taskId: read.summary.taskId,
        commandId: "resume",
        expectedDefinitionRevision: read.summary.definitionRevision,
        expectedLifecycleRevision: read.summary.lifecycleRevision,
        expectedActionPolicyRevision: read.actionPolicyRevision,
        expectedCompletionPolicyRevision: read.completionPolicyRevision,
        expectedManagerGeneration: read.summary.execution.managerGeneration ?? null,
      }),
    ).toBeDefined();
    expect(() => detail({ ...snapshot, completionPolicyRevision: undefined })).toThrow();
  });
});

const decodeRegisterWaitResult = Schema.decodeUnknownSync(task.WorkTaskRegisterWaitResult);
const decodeClearWaitResult = Schema.decodeUnknownSync(task.WorkTaskClearWaitResult);
const decodeReportBlockerResult = Schema.decodeUnknownSync(task.WorkTaskReportBlockerResult);

describe("task waits, results and completion claims", () => {
  const command = { taskId: "task", commandId: "command" };
  const guards = { expectedDefinitionRevision: 2, expectedManagerGeneration: 3 };
  const run = { type: "run", threadId: "source-thread", runId: "source-run" };
  const artifact = { type: "artifact", uri: "artifact", hash: "sha256:abc" };
  const source = { type: "source_revision", repository: "repo", revision: "revision" };
  const committed = {
    taskId: "task",
    definitionRevision: 2,
    lifecycleRevision: 1,
    actionPolicyRevision: 1,
    sequence: 4,
  };

  it("requires concrete wait targets and manager guards without taking acting identity", () => {
    const register = Schema.decodeUnknownSync(task.WorkTaskRegisterWaitInput);
    const clear = Schema.decodeUnknownSync(task.WorkTaskClearWaitInput);
    const input = { ...command, ...guards, waitId: "wait", target: run };
    expect(register({ ...input, caller: { type: "manager" } })).toEqual(input);
    expect(register({ ...input, target: { type: "source", sourceId: "event" } })).toBeDefined();
    expect(register({ ...input, target: { type: "result", result: source } })).toBeDefined();
    for (const target of [
      { type: "run", threadId: "thread" },
      { type: "source" },
      { type: "other" },
    ])
      expect(() => register({ ...input, target })).toThrow();
    for (const key of ["expectedDefinitionRevision", "expectedManagerGeneration", "waitId"]) {
      expect(() => register({ ...input, [key]: undefined })).toThrow();
      expect(() => clear({ ...command, ...guards, waitId: "wait", [key]: undefined })).toThrow();
    }
    expect(decodeRegisterWaitResult({ ...committed, waitId: "wait" })).toBeDefined();
    expect(decodeClearWaitResult({ ...committed, waitId: "wait" })).toBeDefined();
  });

  it("bounds result batches and requires source and server-attributed caller", () => {
    const submit = Schema.decodeUnknownSync(task.WorkTaskSubmitResultsInput);
    const input = {
      ...command,
      expectedDefinitionRevision: 2,
      sourceId: "event",
      summary: "Results",
      results: [run, artifact, source],
    };
    const register = Schema.decodeUnknownSync(task.WorkTaskRegisterWaitInput);
    const claim = Schema.decodeUnknownSync(task.WorkTaskSubmitCompletionClaimInput);
    const referenceInputs = [
      (reference: typeof run) =>
        register({ ...command, ...guards, waitId: "wait", target: reference }),
      (reference: typeof run) =>
        register({
          ...command,
          ...guards,
          waitId: "wait",
          target: { type: "result", result: reference },
        }),
      (reference: typeof run) => submit({ ...input, results: [reference] }),
      (reference: typeof run) =>
        claim({
          ...command,
          ...guards,
          expectedCompletionPolicyRevision: 4,
          summary: "Ready",
          evidence: [reference],
        }),
    ];
    for (const decode of referenceInputs) {
      expect(
        decode({ ...run, threadId: "t".repeat(2_000), runId: "r".repeat(2_000) }),
      ).toBeDefined();
      for (const key of ["threadId", "runId"])
        expect(() => decode({ ...run, [key]: "x".repeat(2_001) })).toThrow();
    }
    expect(submit({ ...input, caller: { type: "manager" } })).toEqual(input);
    expect(
      submit({ ...input, results: Array.from({ length: 32 }, () => ({ ...run })) }).results,
    ).toHaveLength(32);
    for (const results of [
      [],
      Array.from({ length: 33 }, () => ({ ...run })),
      [{ type: "run", runId: "run" }],
    ])
      expect(() => submit({ ...input, results })).toThrow();
    for (const key of ["sourceId", "expectedDefinitionRevision", "summary"])
      expect(() => submit({ ...input, [key]: undefined })).toThrow();
    const result = Schema.decodeUnknownSync(task.WorkTaskSubmitResultsResult);
    const attribution = {
      type: "agent",
      threadId: "actor-thread",
      providerSessionId: "session",
      runId: "actor-run",
    };
    expect(result({ ...committed, sourceId: "event", attribution }).attribution).toEqual(
      attribution,
    );
    expect(() => result({ ...committed, sourceId: "event" })).toThrow();
    expect(() =>
      result({
        ...committed,
        sourceId: "event",
        attribution: { ...attribution, runId: undefined },
      }),
    ).toThrow();
  });

  it("binds blockers and completion claims to current revision and generation and immutable evidence", () => {
    const blocker = Schema.decodeUnknownSync(task.WorkTaskReportBlockerInput);
    const claim = Schema.decodeUnknownSync(task.WorkTaskSubmitCompletionClaimInput);
    const input = {
      ...command,
      ...guards,
      expectedCompletionPolicyRevision: 4,
      summary: "Ready",
      evidence: [run, artifact, source],
    };
    expect(
      blocker({ ...command, ...guards, reason: "Waiting for access", caller: "manager" }),
    ).toEqual({ ...command, ...guards, reason: "Waiting for access" });
    expect(claim({ ...input, caller: "manager", status: "done" })).toEqual(input);
    for (const key of [
      "expectedDefinitionRevision",
      "expectedManagerGeneration",
      "expectedCompletionPolicyRevision",
      "summary",
      "evidence",
    ])
      expect(() => claim({ ...input, [key]: undefined })).toThrow();
    for (const evidence of [
      [],
      Array.from({ length: 33 }, () => ({ ...run })),
      [{ type: "artifact", uri: "path" }],
    ])
      expect(() => claim({ ...input, evidence })).toThrow();
    expect(
      claim({ ...input, evidence: Array.from({ length: 32 }, () => ({ ...artifact })) }).evidence,
    ).toHaveLength(32);
    expect(() => blocker({ ...command, ...guards, reason: " " })).toThrow();
    expect(decodeReportBlockerResult(committed)).toEqual(committed);
    const result = Schema.decodeUnknownSync(task.WorkTaskSubmitCompletionClaimResult);
    const receipt = {
      ...committed,
      managerGeneration: 3,
      completionPolicyRevision: 4,
      claimId: "claim",
      evidenceId: "evidence",
    };
    expect(result(receipt)).toEqual(receipt);
    for (const key of [
      "definitionRevision",
      "managerGeneration",
      "completionPolicyRevision",
      "claimId",
      "evidenceId",
    ])
      expect(() => result({ ...receipt, [key]: undefined })).toThrow();
  });
});

describe("task action review contracts", () => {
  const preview = {
    content: "Complete action",
    artifacts: [{ uri: "artifact", hash: "sha256:abc" }],
  };
  const proposal = {
    action: "git.push",
    label: "Push",
    target: "origin/main",
    preview,
    fingerprint: "sha256:123",
  };
  const request = {
    taskId: "task",
    commandId: "command",
    expectedDefinitionRevision: 1,
    expectedActionPolicyRevision: 2,
    expectedManagerGeneration: 3,
    ...proposal,
  };
  const review = {
    taskId: "task",
    reviewId: "review",
    revision: 1,
    managerThreadId: "thread",
    managerGeneration: 3,
    providerSessionId: "session",
    runId: "run",
    ruleId: "rule",
    definitionRevision: 1,
    actionPolicyRevision: 2,
    ...proposal,
    state: { type: "pending" },
    createdSequence: 4,
    createdAt: "2026-11-11T00:00:00Z",
  };
  const response = {
    taskId: "task",
    commandId: "command",
    reviewId: "review",
    expectedReviewRevision: 1,
    expectedActionPolicyRevision: 2,
    expectedManagerGeneration: 3,
    expectedFingerprint: "sha256:123",
    decision: "approve",
  };

  it("bounds the complete serialized preview in UTF-8 bytes", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskActionPreview);
    const overhead = new TextEncoder().encode(
      JSON.stringify({ content: "", artifacts: [] }),
    ).byteLength;
    const limit = 32 * 1024 - overhead;
    expect(decode({ content: "a".repeat(limit), artifacts: [] })).toBeDefined();
    expect(() => decode({ content: "a".repeat(limit + 1), artifacts: [] })).toThrow();
    expect(
      decode({ content: "é".repeat(Math.floor(limit / 2)) + "a".repeat(limit % 2), artifacts: [] }),
    ).toBeDefined();
    expect(() =>
      decode({
        content: "é".repeat(Math.floor(limit / 2)) + "a".repeat((limit % 2) + 1),
        artifacts: [],
      }),
    ).toThrow();
    expect(() => decode({ content: '"'.repeat(limit), artifacts: [] })).toThrow();
    expect(() => decode({ content: " ", artifacts: [] })).toThrow();
  });

  it("bounds artifact count and every review reference and text field", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskActionPreview);
    const artifact = { uri: "u".repeat(2_000), hash: "h".repeat(2_000) };
    expect(
      decode({
        content: "x",
        artifacts: Array.from({ length: 16 }, () => ({ uri: "u", hash: "h" })),
      }),
    ).toBeDefined();
    expect(() =>
      decode({
        content: "x",
        artifacts: Array.from({ length: 17 }, () => ({ uri: "u", hash: "h" })),
      }),
    ).toThrow();
    expect(decode({ content: "x", artifacts: [artifact] })).toBeDefined();
    for (const key of ["uri", "hash"])
      expect(() =>
        decode({ content: "x", artifacts: [{ ...artifact, [key]: "x".repeat(2_001) }] }),
      ).toThrow();
    const requestDecode = Schema.decodeUnknownSync(task.WorkTaskRequestActionReviewInput);
    for (const key of ["taskId", "commandId", "label", "target", "fingerprint"]) {
      expect(requestDecode({ ...request, [key]: "x".repeat(2_000) })).toBeDefined();
      expect(() => requestDecode({ ...request, [key]: "x".repeat(2_001) })).toThrow();
    }
    expect(() => requestDecode({ ...request, action: "x".repeat(121) })).toThrow();
    const responseDecode = Schema.decodeUnknownSync(task.WorkTaskRespondActionReviewInput);
    for (const key of ["reviewId", "expectedFingerprint", "feedback"]) {
      expect(responseDecode({ ...response, [key]: "x".repeat(2_000) })).toBeDefined();
      expect(() => responseDecode({ ...response, [key]: "x".repeat(2_001) })).toThrow();
    }
  });

  it("binds review request, read and human decision to exact revisions and payload without caller authority", () => {
    const requestDecode = Schema.decodeUnknownSync(task.WorkTaskRequestActionReviewInput);
    expect(requestDecode({ ...request, caller: { type: "manager" } })).toEqual(request);
    for (const key of [
      "expectedDefinitionRevision",
      "expectedActionPolicyRevision",
      "expectedManagerGeneration",
      "fingerprint",
      "preview",
    ]) {
      expect(() => requestDecode({ ...request, [key]: undefined })).toThrow();
    }
    const respond = Schema.decodeUnknownSync(task.WorkTaskRespondActionReviewInput);
    for (const decision of ["approve", "reject", "request_changes"])
      expect(respond({ ...response, decision, caller: { type: "user" } })).toEqual({
        ...response,
        decision,
      });
    for (const decision of ["allow", "always", "session", "changes_requested", "agent_approve"])
      expect(() => respond({ ...response, decision })).toThrow();
    for (const key of [
      "reviewId",
      "expectedReviewRevision",
      "expectedActionPolicyRevision",
      "expectedManagerGeneration",
      "expectedFingerprint",
      "decision",
    ])
      expect(() => respond({ ...response, [key]: undefined })).toThrow();
    const detail = Schema.decodeUnknownSync(task.WorkTaskActionReviewDetailInput);
    expect(detail({ taskId: "task", reviewId: "review" })).toEqual({
      taskId: "task",
      reviewId: "review",
    });
    expect(() => detail({ taskId: "task" })).toThrow();
    for (const schema of [
      task.WorkTaskActionReviewDetailResult,
      task.WorkTaskRequestActionReviewResult,
      task.WorkTaskRespondActionReviewResult,
    ]) {
      const decode = Schema.decodeUnknownSync(schema);
      expect(decode({ sequence: 4, review })).toBeDefined();
      for (const key of ["fingerprint", "actionPolicyRevision", "managerGeneration", "revision"])
        expect(() => decode({ sequence: 4, review: { ...review, [key]: undefined } })).toThrow();
    }
    const state = Schema.decodeUnknownSync(task.WorkTaskActionReviewState);
    for (const type of ["approved", "rejected", "changes_requested"]) {
      expect(
        state({
          type,
          subject: "human",
          sessionId: "session",
          resolvedSequence: 5,
          resolvedAt: "2026-11-11T00:00:00Z",
        }),
      ).toBeDefined();
      expect(() => state({ type, resolvedSequence: 5 })).toThrow();
    }
    for (const type of ["invalidated", "cancelled"])
      expect(
        state({ type, reason: "changed", resolvedSequence: 5, resolvedAt: "2026-11-11T00:00:00Z" }),
      ).toBeDefined();
    expect(() => state({ type: "approve" })).toThrow();
  });
});
