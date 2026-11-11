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
const decodeAdoptDefinition = Schema.decodeUnknownSync(WorkTaskAdoptDefinitionInput);
const decodeResume = Schema.decodeUnknownSync(task.WorkTaskResumeInput);

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
    archived: false,
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
