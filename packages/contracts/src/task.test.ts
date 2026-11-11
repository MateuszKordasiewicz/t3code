import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerConfig } from "./server.ts";

import {
  WorkTaskAcceptedDefinition,
  WorkTaskActionPolicy,
  WorkTaskDraftDefinition,
  WorkTaskError,
  WorkTaskListInput,
  WorkTaskListResult,
  WorkTaskMutationInput,
  WorkTaskMutationResult,
  WorkTaskSubscriptionEvent,
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
    expect(() => decode({ _tag: "WorkTaskError", code: "other", message: "bad" })).toThrow();
  });
});


describe("work task API", () => {
  it("leaves work tasks unavailable when the capability is absent", () => {
    const decode = Schema.decodeUnknownSync(Schema.Struct({ workTasks: ServerConfig.fields.workTasks }));
    expect(decode({}).workTasks).toBeUndefined();
    expect(decode({ workTasks: true }).workTasks).toBe(true);
  });
  it("bounds pages and carries a snapshot sequence", () => {
    const input = Schema.decodeUnknownSync(WorkTaskListInput);
    expect(input({ limit: 100 })).toEqual({ limit: 100 });
    for (const limit of [0, 101]) expect(() => input({ limit })).toThrow();
    const result = Schema.decodeUnknownSync(WorkTaskListResult);
    expect(result({ tasks: [], nextPageToken: null, sequence: 0 }).sequence).toBe(0);
    expect(() => result({ tasks: Array(101).fill({ id: "t", title: "T", projectId: null, mode: "active", revision: 1 }), nextPageToken: null, sequence: 0 })).toThrow();
  });

  it("keeps caller identity out of mutation payloads and requires revisions", () => {
    const decode = Schema.decodeUnknownSync(WorkTaskMutationInput);
    expect(decode({ type: "pause", taskId: "t", expectedRevision: 1, commandId: "c", actor: { type: "user" } })).toEqual({ type: "pause", taskId: "t", expectedRevision: 1, commandId: "c" });
    expect(() => decode({ type: "pause", taskId: "t", commandId: "c" })).toThrow();
    expect(Schema.decodeUnknownSync(WorkTaskMutationResult)({ taskId: "t", revision: 2, sequence: 3 }).sequence).toBe(3);
  });

  it("distinguishes invalidation from an explicit resync", () => {
    const decode = Schema.decodeUnknownSync(WorkTaskSubscriptionEvent);
    expect(decode({ type: "resync-required", sequence: 4, reason: "buffer-overflow" }).type).toBe("resync-required");
    expect(() => decode({ type: "resync-required", sequence: 4, reason: "partial" })).toThrow();
  });
});
