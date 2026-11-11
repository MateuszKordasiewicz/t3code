import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerConfig } from "./server.ts";
import * as task from "./task.ts";

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
    for (const code of ["feature_disabled", "unsupported", "unavailable"]) {
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
    execution: { state: "waiting" },
    attention: { state: "none" },
    completion: "incomplete",
  };
  const reference = { type: "run", threadId: "thread", runId: "run" };
  const contribution = {
    id: "result",
    threadId: "thread",
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
    expect(detail({ sequence: 1, summary, definition })).toBeDefined();
    expect(() => detail({ summary, definition })).toThrow();
    expect(() => decodeDetailInput({})).toThrow();
  });

  it("separates desired mode, execution, attention and completion and bounds text", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskSummary);
    expect(
      decode({
        ...summary,
        desiredMode: "paused",
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
