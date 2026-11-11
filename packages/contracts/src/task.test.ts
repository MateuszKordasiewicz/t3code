import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerConfig } from "./server.ts";

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
