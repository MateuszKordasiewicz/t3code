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

  it("records distinct manager binding and retirement identities for custody replay", () => {
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = { sequence: 8, eventId: "event", occurredAt: base.occurredAt, commandId: null };
    const bound = {
      ...base,
      type: "task.manager-bound",
      managerThreadId: "manager",
      managerGeneration: 1,
    };
    const retired = { ...bound, type: "task.manager-retired", lifecycleRevision: 9 };
    const replacement = {
      ...bound,
      managerThreadId: "replacement",
      managerGeneration: 2,
      definitionRevision: 7,
    };
    for (const event of [bound, retired, replacement]) {
      expect(decode(event)).toEqual(event);
      expect(persisted({ event, context })).toEqual({ event, context });
      for (const field of [
        "id",
        "taskId",
        "occurredAt",
        "attribution",
        "managerThreadId",
        "managerGeneration",
        "definitionRevision",
        "lifecycleRevision",
        "completionPolicyRevision",
        "actionPolicyRevision",
      ])
        expect(() => decode({ ...event, [field]: undefined })).toThrow();
      for (const managerGeneration of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Infinity])
        expect(() => decode({ ...event, managerGeneration })).toThrow();
      for (const field of ["id", "taskId", "managerThreadId"])
        for (const value of [" ", "x".repeat(2_001)])
          expect(() => decode({ ...event, [field]: value })).toThrow();
      expect(() => decode({ ...event, occurredAt: "2026-02-30T00:00:00Z" })).toThrow();
      expect(() => decode({ ...event, attribution: { type: "user", subject: "human" } })).toThrow();
      expect(() => persisted({ event, context: { ...context, eventId: "other" } })).toThrow();
      expect(() =>
        persisted({ event, context: { ...context, occurredAt: "2026-11-12T00:00:00Z" } }),
      ).toThrow();
    }
    expect(decode({ ...bound, managerGeneration: Number.MAX_SAFE_INTEGER })).toBeDefined();
    expect(() => decode({ ...bound, type: "task.manager-changed" })).toThrow();
    expect(() => decode({ ...retired, managerThreadId: undefined })).toThrow();
  });

  it("provides typed manager binding replay conflicts with concrete identities", () => {
    const decodeConflict = Schema.decodeUnknownSync(task.WorkTaskManagerBindingReplayConflict);
    const conflict = {
      _tag: "WorkTaskManagerBindingReplayConflict",
      code: "binding_identity_conflict",
      taskId: "task",
      managerThreadId: "manager",
      managerGeneration: 1,
    };
    for (const code of [
      "binding_identity_conflict",
      "thread_already_bound",
      "current_binding_conflict",
      "binding_not_found",
      "binding_already_retired",
    ])
      expect(decodeConflict({ ...conflict, code }).code).toBe(code);
    expect(() => decodeConflict({ ...conflict, code: "other" })).toThrow();
    for (const field of ["taskId", "managerThreadId", "managerGeneration"])
      expect(() => decodeConflict({ ...conflict, [field]: undefined })).toThrow();
    expect(() => decodeConflict({ ...conflict, managerGeneration: 0 })).toThrow();
  });

  it("records allowance configuration, top-up and debit with consistent replay state", () => {
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = {
      sequence: 8,
      eventId: "event",
      occurredAt: base.occurredAt,
      commandId: "command",
    };
    const human = { type: "user", subject: "human", sessionId: "session" };
    const prior = { type: "configured", cap: 3, consumed: 1 };
    const configured = {
      ...base,
      type: "task.allowance-configured",
      commandId: "command",
      attribution: human,
      previous: { type: "unconfigured", consumed: 1 },
      resulting: prior,
    };
    const topup = {
      ...configured,
      type: "task.allowance-topped-up",
      previous: prior,
      resulting: { ...prior, cap: 5 },
    };
    const debit = {
      ...configured,
      type: "task.allowance-debited",
      attribution: { type: "server" },
      previous: prior,
      resulting: { ...prior, consumed: 2 },
      allowanceUnit: 2,
    };
    for (const event of [configured, topup, debit]) {
      expect(decode(event)).toEqual(event);
      expect(persisted({ event, context })).toEqual({ event, context });
      for (const field of [
        "id",
        "taskId",
        "occurredAt",
        "attribution",
        "commandId",
        "previous",
        "resulting",
        "definitionRevision",
        "lifecycleRevision",
        "completionPolicyRevision",
        "actionPolicyRevision",
      ])
        expect(() => decode({ ...event, [field]: undefined })).toThrow();
      for (const field of ["id", "taskId", "commandId"])
        for (const value of [" ", "x".repeat(2_001)])
          expect(() => decode({ ...event, [field]: value })).toThrow();
      expect(() => persisted({ event, context: { ...context, commandId: "other" } })).toThrow();
      expect(() => persisted({ event, context: { ...context, eventId: "other" } })).toThrow();
      expect(() =>
        persisted({ event, context: { ...context, occurredAt: "2026-11-12T00:00:00Z" } }),
      ).toThrow();
      expect(() => decode({ ...event, occurredAt: "2026-02-30T00:00:00Z" })).toThrow();
    }
    expect(decode({ ...configured, previous: prior })).toBeDefined();
    expect(decode({ ...configured, attribution: { type: "server" } })).toBeDefined();
    for (const change of [
      { previous: { type: "unconfigured", consumed: 2 } },
      { resulting: { ...prior, cap: 0 } },
      { resulting: { ...prior, cap: 1, consumed: 2 } },
      {
        attribution: {
          type: "manager",
          threadId: "thread",
          providerSessionId: "session",
          runId: "run",
          generation: 1,
        },
      },
    ])
      expect(() => decode({ ...configured, ...change })).toThrow();
    for (const change of [
      { previous: { type: "unconfigured", consumed: 1 } },
      { resulting: { ...prior, cap: 3 } },
      { resulting: { ...prior, cap: 5, consumed: 0 } },
      { attribution: { type: "server" } },
    ])
      expect(() => decode({ ...topup, ...change })).toThrow();
    for (const change of [
      { allowanceUnit: undefined },
      { allowanceUnit: 1 },
      { resulting: { ...prior, consumed: 1 } },
      { resulting: { cap: 4, consumed: 2, type: "configured" } },
      { attribution: human },
    ])
      expect(() => decode({ ...debit, ...change })).toThrow();
    for (const value of [-1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        decode({ ...configured, previous: { type: "unconfigured", consumed: value } }),
      ).toThrow();
      expect(() => decode({ ...configured, resulting: { ...prior, cap: value } })).toThrow();
      expect(() => decode({ ...debit, allowanceUnit: value })).toThrow();
    }
    expect(() => decode({ ...debit, allowanceUnit: 0 })).toThrow();
    expect(
      decode({
        ...debit,
        previous: { ...prior, cap: Number.MAX_SAFE_INTEGER, consumed: Number.MAX_SAFE_INTEGER - 1 },
        resulting: { ...prior, cap: Number.MAX_SAFE_INTEGER, consumed: Number.MAX_SAFE_INTEGER },
        allowanceUnit: Number.MAX_SAFE_INTEGER,
      }),
    ).toBeDefined();
  });

  it("stages wake sources, reservations, claims and observed acknowledgements for replay", () => {
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const context = {
      sequence: 8,
      eventId: "event",
      occurredAt: base.occurredAt,
      commandId: "change",
    };
    const source = { kind: "run", sourceId: "source", resultRef: "result" };
    const recorded = {
      ...base,
      type: "task.wake-source-recorded",
      commandId: "change",
      source,
      reservationId: null,
      dirtyGeneration: 1,
    };
    const common = {
      ...base,
      commandId: "change",
      reservationId: "reservation",
      reservationCommandId: "wake-command",
      managerGeneration: 1,
      allowanceUnit: 2,
      dirtyGeneration: 3,
      acknowledgedGeneration: 1,
    };
    const pending = {
      sources: [source, { kind: "check", sourceId: "other", resultRef: null }],
      overflowThroughGeneration: 3,
    };
    const reserved = { ...common, type: "task.wake-reserved", pending };
    const claimed = {
      ...common,
      type: "task.wake-claimed",
      messageId: "message",
      runId: "run",
      pending,
    };
    const uncertain = { ...common, type: "task.wake-claim-uncertain", reason: "outcome unknown" };
    const acknowledged = {
      ...common,
      type: "task.wake-acknowledged",
      observedDirtyGeneration: 2,
      previousAcknowledgedGeneration: 0,
      acknowledgedGeneration: 2,
    };
    for (const event of [recorded, reserved, claimed, uncertain, acknowledged]) {
      expect(decode(event)).toEqual(event);
      expect(persisted({ event, context })).toEqual({ event, context });
      for (const field of ["id", "taskId", "occurredAt", "commandId", "attribution"]) {
        expect(() => decode({ ...event, [field]: undefined })).toThrow();
      }
      for (const change of [
        { commandId: null },
        { commandId: "other" },
        { eventId: "other" },
        { occurredAt: "2026-11-12T00:00:00Z" },
      ]) {
        expect(() => persisted({ event, context: { ...context, ...change } })).toThrow();
      }
    }
    for (const field of [
      "reservationId",
      "reservationCommandId",
      "managerGeneration",
      "allowanceUnit",
      "dirtyGeneration",
      "acknowledgedGeneration",
      "pending",
    ]) {
      expect(() => decode({ ...reserved, [field]: undefined })).toThrow();
    }
    for (const field of ["messageId", "runId"])
      expect(() => decode({ ...claimed, [field]: undefined })).toThrow();
    expect(() => decode({ ...uncertain, reason: undefined })).toThrow();
    for (const field of ["observedDirtyGeneration", "previousAcknowledgedGeneration"])
      expect(() => decode({ ...acknowledged, [field]: undefined })).toThrow();
    for (const field of ["reservationId", "reservationCommandId", "taskId"])
      for (const value of [" ", "x".repeat(2001)])
        expect(() => decode({ ...reserved, [field]: value })).toThrow();
    for (const field of ["managerGeneration", "allowanceUnit", "dirtyGeneration"])
      for (const value of [0, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1])
        expect(() => decode({ ...reserved, [field]: value })).toThrow();
    for (const change of [
      { acknowledgedGeneration: 4 },
      { acknowledgedGeneration: -1 },
      { observedDirtyGeneration: 1 },
      { observedDirtyGeneration: 4 },
      { previousAcknowledgedGeneration: 3 },
    ])
      expect(() => decode({ ...acknowledged, ...change })).toThrow();
    expect(() =>
      decode({ ...reserved, pending: { ...pending, overflowThroughGeneration: 4 } }),
    ).toThrow();
    expect(() =>
      decode({ ...reserved, pending: { ...pending, sources: [source, source] } }),
    ).toThrow();
    expect(() =>
      decode({
        ...reserved,
        pending: {
          ...pending,
          sources: Array.from({ length: 33 }, (_, i) => ({ ...source, sourceId: String(i) })),
        },
      }),
    ).toThrow();
    expect(() => decode({ ...recorded, source: { ...source, sourceId: " " } })).toThrow();
    expect(decode({ ...recorded, reservationId: "reservation" })).toBeDefined();
    expect(
      decode({ ...reserved, pending: { sources: [], overflowThroughGeneration: null } }),
    ).toBeDefined();
  });

  it("records reserved, started, result and uncertain validation attempts with replay bindings", () => {
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedEvent);
    const common = {
      ...base,
      commandId: "command",
      attemptId: "attempt",
      managerGeneration: 1,
      claimId: "claim",
      evidenceId: "evidence",
      workWatermark: "watermark",
      validator: {
        projectId: "project",
        modelSelection: { instanceId: "codex", model: "model" },
        workspace: { type: "root" },
      },
      allowanceUnit: 1,
      createdAt: base.occurredAt,
    };
    const reserved = { ...common, type: "task.validation-reserved" };
    const started = {
      ...common,
      type: "task.validation-started",
      runId: "run",
      startedAt: base.occurredAt,
    };
    const result = {
      ...started,
      type: "task.validation-result",
      finishedAt: base.occurredAt,
      outcome: "pass",
      findings: [
        {
          criterionId: "criterion-1",
          outcome: "pass",
          reason: "verified",
          evidence: [{ type: "artifact", uri: "file", hash: "hash" }],
        },
      ],
    };
    const uncertain = {
      ...started,
      type: "task.validation-uncertain",
      finishedAt: base.occurredAt,
      reason: "process lost",
    };
    const context = {
      sequence: 8,
      eventId: "event",
      occurredAt: base.occurredAt,
      commandId: "command",
    };
    for (const event of [reserved, started, result, uncertain]) {
      expect(decode(event)).toEqual(event);
      expect(persisted({ event, context })).toEqual({ event, context });
      expect(() => persisted({ event, context: { ...context, commandId: "other" } })).toThrow();
      for (const field of [
        "attemptId",
        "taskId",
        "definitionRevision",
        "managerGeneration",
        "completionPolicyRevision",
        "claimId",
        "evidenceId",
        "workWatermark",
        "validator",
        "allowanceUnit",
        "createdAt",
      ])
        expect(() => decode({ ...event, [field]: undefined })).toThrow();
    }
    for (const field of ["runId", "startedAt"])
      expect(() => decode({ ...started, [field]: undefined })).toThrow();
    for (const field of ["finishedAt", "outcome", "findings"])
      expect(() => decode({ ...result, [field]: undefined })).toThrow();
    for (const field of ["finishedAt", "reason"])
      expect(() => decode({ ...uncertain, [field]: undefined })).toThrow();
    for (const change of [
      { runId: "run" },
      { startedAt: base.occurredAt },
      { outcome: "pass" },
      { reason: "x" },
    ])
      expect(() => decode({ ...reserved, ...change })).toThrow();
    for (const change of [{ outcome: "pass" }, { finishedAt: base.occurredAt }, { reason: "x" }])
      expect(() => decode({ ...started, ...change })).toThrow();
    expect(() => decode({ ...uncertain, outcome: "pass" })).toThrow();
    expect(
      decode({ ...result, outcome: "error", findings: [], reason: "provider failed" }),
    ).toBeDefined();
    expect(() => decode({ ...result, outcome: "error", findings: [] })).toThrow();
    expect(() => decode({ ...result, findings: [] })).toThrow();
    expect(() => decode({ ...result, outcome: "fail" })).toThrow();
    expect(() =>
      decode({ ...result, findings: [result.findings[0], result.findings[0]] }),
    ).toThrow();
    expect(() => decode({ ...started, createdAt: "2026-11-12T00:00:00Z" })).toThrow();
    const earlier = "2026-11-11T00:00:00.0001Z";
    const later = "2026-11-11T00:00:00.0009Z";
    for (const event of [
      { ...started, createdAt: later, startedAt: earlier, occurredAt: earlier },
      { ...result, createdAt: earlier, startedAt: later, finishedAt: earlier, occurredAt: earlier },
      {
        ...uncertain,
        createdAt: earlier,
        startedAt: later,
        finishedAt: earlier,
        occurredAt: earlier,
      },
    ]) {
      expect(() => decode(event)).toThrow();
      expect(() => persisted({ event, context: { ...context, occurredAt: earlier } })).toThrow();
    }
    for (const event of [result, uncertain]) {
      const precise = {
        ...event,
        createdAt: earlier,
        startedAt: earlier,
        finishedAt: later,
        occurredAt: later,
      };
      expect(decode(precise)).toEqual(precise);
      expect(
        persisted({ event: precise, context: { ...context, occurredAt: later } }),
      ).toBeDefined();
    }
    const obsolete = {
      ...base,
      type: "task.validation-recorded",
      managerThreadId: "manager",
      managerGeneration: 1,
      claimId: "claim",
      evidenceId: "evidence",
      observedWorkSequence: 1,
      attemptId: "attempt",
      validator: common.validator,
      outcome: "pass",
      findings: [],
    };
    expect(() => decode(obsolete)).toThrow();
    expect(() =>
      persisted({ event: obsolete, context: { ...context, commandId: null } }),
    ).toThrow();

    for (const field of ["attemptId", "claimId", "evidenceId", "workWatermark"])
      for (const value of [" ", "x".repeat(2001)])
        expect(() => decode({ ...reserved, [field]: value })).toThrow();
    for (const field of [
      "managerGeneration",
      "definitionRevision",
      "completionPolicyRevision",
      "allowanceUnit",
    ])
      for (const value of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
        expect(() => decode({ ...reserved, [field]: value })).toThrow();
    expect(() => decode({ ...result, findings: Array(65).fill(result.findings[0]) })).toThrow();
    expect(() =>
      decode({ ...result, findings: [{ ...result.findings[0], outcome: "error" }] }),
    ).toThrow();
    expect(() => decode({ ...uncertain, reason: " " })).toThrow();
    expect(() => decode({ ...started, startedAt: "invalid" })).toThrow();
    expect(() =>
      decode({
        ...reserved,
        attribution: {
          type: "manager",
          threadId: "t",
          providerSessionId: "p",
          runId: "r",
          generation: 1,
        },
      }),
    ).toThrow();
  });

  it("provides typed validation replay conflicts", () => {
    const decodeConflict = Schema.decodeUnknownSync(task.WorkTaskValidationReplayConflict);
    const value = {
      _tag: "WorkTaskValidationReplayConflict",
      code: "attempt_identity_conflict",
      taskId: "task",
      attemptId: "attempt",
    };
    expect(decodeConflict(value).code).toBe(value.code);
    expect(() => decodeConflict({ ...value, code: "other" })).toThrow();
    expect(() => decodeConflict({ ...value, attemptId: " " })).toThrow();
  });

  it("provides typed wake replay conflicts", () => {
    const conflict = Schema.decodeUnknownSync(task.WorkTaskWakeReplayConflict);
    const value = {
      _tag: "WorkTaskWakeReplayConflict",
      taskId: "task",
      reservationId: "reservation",
      code: "source_identity_conflict",
    };
    for (const code of [
      "source_identity_conflict",
      "reservation_identity_conflict",
      "command_identity_conflict",
      "allowance_unit_conflict",
      "pending_reservation_conflict",
      "reservation_not_found",
      "claim_identity_conflict",
      "generation_conflict",
      "acknowledgement_conflict",
    ])
      expect(conflict({ ...value, code }).code).toBe(code);
    expect(conflict({ ...value, reservationId: null }).reservationId).toBeNull();
    expect(() => conflict({ ...value, code: "other" })).toThrow();
    expect(() => conflict({ ...value, reservationId: " " })).toThrow();
  });

  it("provides typed allowance replay conflicts bound to task and command", () => {
    const decodeConflict = Schema.decodeUnknownSync(task.WorkTaskAllowanceReplayConflict);
    const conflict = {
      _tag: "WorkTaskAllowanceReplayConflict",
      taskId: "task",
      commandId: "command",
    };
    for (const code of [
      "prior_state_mismatch",
      "allowance_not_configured",
      "cap_below_consumed",
      "allowance_exhausted",
      "unit_identity_conflict",
      "command_identity_conflict",
    ])
      expect(decodeConflict({ ...conflict, code }).code).toBe(code);
    expect(() => decodeConflict({ ...conflict, code: "other" })).toThrow();
    for (const field of ["taskId", "commandId"])
      for (const value of [undefined, " ", "x".repeat(2_001)])
        expect(() =>
          decodeConflict({ ...conflict, code: "prior_state_mismatch", [field]: value }),
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

const decodeGraphProposalInput = Schema.decodeUnknownSync(task.WorkTaskCreateBlockerProposalInput);
const decodeGraphAdoptionInput = Schema.decodeUnknownSync(task.WorkTaskAddBlockerInput);
const decodeGraphReadInput = Schema.decodeUnknownSync(task.WorkTaskGraphReadInput);
const decodeGraphPage = Schema.decodeUnknownSync(task.WorkTaskGraphReadResult);
const decodeGraphRef = Schema.decodeUnknownSync(task.WorkTaskGraphTaskRef);
const decodeGraphProposalRecord = Schema.decodeUnknownSync(task.WorkTaskGraphProposalRecord);
const decodeGraphAddResult = Schema.decodeSync(task.WorkTaskAddBlockerResult);
const decodeGraphRemoveResult = Schema.decodeSync(task.WorkTaskRemoveBlockerResult);
const decodeGraphProposalResult = Schema.decodeSync(task.WorkTaskCreateBlockerProposalResult);
const decodeExistingTaskId = Schema.decodeSync(task.WorkTaskId);

it("bounds passive dependency contracts without accepting caller authority", () => {
  const ref = { environmentId: "local", taskId: "a" };
  const blocker = { environmentId: "local", taskId: "b" };
  const proposal = { dependent: ref, blocker, observedGraphRevision: 0, reason: "needed" };
  expect(
    decodeGraphProposalInput({ ...proposal, actor: "human", ready: true, grant: "approve" }),
  ).toEqual(proposal);
  expect(
    decodeGraphAdoptionInput({
      dependent: ref,
      blocker,
      expectedGraphRevision: 0,
      authority: "human",
    }),
  ).toEqual({
    dependent: ref,
    blocker,
    expectedGraphRevision: 0,
  });
  for (const endpoint of [ref, { environmentId: " local ", taskId: " a " }]) {
    expect(() =>
      decodeGraphAdoptionInput({ dependent: ref, blocker: endpoint, expectedGraphRevision: 0 }),
    ).toThrow();
    expect(() => decodeGraphProposalInput({ ...proposal, blocker: endpoint })).toThrow();
  }
  expect(
    decodeGraphAdoptionInput({
      dependent: ref,
      blocker: { environmentId: "remote", taskId: "a" },
      expectedGraphRevision: 0,
    }).blocker.environmentId,
  ).toBe("remote");
  for (const key of ["environmentId", "taskId"] as const) {
    expect(decodeGraphRef({ ...ref, [key]: "x".repeat(2_000) })[key]).toHaveLength(2_000);
    for (const value of [" ", "x".repeat(2_001)]) {
      expect(() => decodeGraphRef({ ...ref, [key]: value })).toThrow();
    }
  }
  expect(decodeExistingTaskId("x".repeat(2_001))).toHaveLength(2_001);
  for (const revision of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Infinity]) {
    expect(() =>
      decodeGraphAdoptionInput({ dependent: ref, blocker, expectedGraphRevision: revision }),
    ).toThrow();
  }
  expect(
    decodeGraphAdoptionInput({
      dependent: ref,
      blocker,
      expectedGraphRevision: Number.MAX_SAFE_INTEGER,
    }).expectedGraphRevision,
  ).toBe(Number.MAX_SAFE_INTEGER);
  expect(decodeGraphReadInput({ task: ref, direction: "dependents" })).toEqual({
    task: ref,
    direction: "dependents",
  });
  for (const limit of [0, 101, 0.5])
    expect(() => decodeGraphReadInput({ task: ref, direction: "blockers", limit })).toThrow();
  expect(
    decodeGraphReadInput({ task: ref, direction: "blockers", limit: 100, pageToken: "opaque" })
      .pageToken,
  ).toBe("opaque");
  expect(() =>
    decodeGraphReadInput({ task: ref, direction: "blockers", pageToken: "x".repeat(2_001) }),
  ).toThrow();
  const provenance = {
    eventId: "edge-event",
    commandId: "command",
    occurredAt: "2026-11-15T06:00:00Z",
    adoptedBy: { subject: "user", sessionId: "session" },
  };
  const edge = { edgeId: "edge", dependent: ref, blocker, createdGraphRevision: 1, provenance };
  const snapshot = {
    sequence: 1,
    graphRevision: 1,
    viewGeneration: 2,
    edges: [edge],
    nextPageToken: "opaque",
  };
  expect(decodeGraphPage(snapshot)).toEqual(snapshot);
  expect(() => decodeGraphPage({ ...snapshot, graphRevision: 0 })).toThrow();
  expect(() =>
    decodeGraphPage({
      ...snapshot,
      edges: [{ ...edge, blocker: { ...blocker, environmentId: "remote" } }],
    }),
  ).toThrow();
  expect(() => decodeGraphAddResult({ sequence: 1, graphRevision: 0, edge })).toThrow();
  expect(() =>
    decodeGraphPage({
      ...snapshot,
      edges: [{ ...edge, provenance: { ...provenance, occurredAt: "not-a-timestamp" } }],
    }),
  ).toThrow();
  expect(
    decodeGraphPage({
      ...snapshot,
      edges: Array.from({ length: 100 }, (_, i) => ({ ...edge, edgeId: `edge-${i}` })),
    }).edges,
  ).toHaveLength(100);
  expect(() =>
    decodeGraphPage({ ...snapshot, edges: Array.from({ length: 101 }, () => edge) }),
  ).toThrow();
  expect(() =>
    decodeGraphPage({ ...snapshot, edges: [{ ...edge, createdGraphRevision: 0 }] }),
  ).toThrow();
  expect(() =>
    decodeGraphPage({ ...snapshot, viewGeneration: Number.MAX_SAFE_INTEGER + 1 }),
  ).toThrow();
  for (const key of ["eventId", "commandId", "occurredAt"] as const) {
    expect(() =>
      decodeGraphPage({
        ...snapshot,
        edges: [{ ...edge, provenance: { ...provenance, [key]: "x".repeat(2_001) } }],
      }),
    ).toThrow();
  }
  for (const key of ["subject", "sessionId"] as const) {
    expect(() =>
      decodeGraphPage({
        ...snapshot,
        edges: [
          {
            ...edge,
            provenance: {
              ...provenance,
              adoptedBy: { ...provenance.adoptedBy, [key]: "x".repeat(2_001) },
            },
          },
        ],
      }),
    ).toThrow();
  }
  const proposalRecord = {
    proposalId: "proposal",
    dependent: ref,
    blocker,
    observedGraphRevision: 0,
    proposedAt: provenance.occurredAt,
    proposedBy: {
      type: "agent" as const,
      threadId: "thread",
      providerSessionId: "provider",
      runId: "run",
    },
  };
  expect(
    decodeGraphProposalRecord({ ...proposalRecord, status: "approved", grant: "bearer" }),
  ).toEqual(proposalRecord);
  for (const key of ["threadId", "providerSessionId", "runId"] as const) {
    expect(() =>
      decodeGraphProposalRecord({
        ...proposalRecord,
        proposedBy: { ...proposalRecord.proposedBy, [key]: "x".repeat(2_001) },
      }),
    ).toThrow();
  }
  expect(() =>
    decodeGraphProposalRecord({ ...proposalRecord, proposedBy: { type: "human" } }),
  ).toThrow();
  expect(decodeGraphAddResult({ sequence: 1, graphRevision: 1, edge }).edge).toEqual(edge);
  expect(
    decodeGraphRemoveResult({
      sequence: 2,
      graphRevision: 2,
      edgeId: "edge",
      provenance,
    }).graphRevision,
  ).toBe(2);
  expect(
    decodeGraphProposalResult({
      sequence: 1,
      proposal: proposalRecord,
    }).proposal,
  ).toEqual(proposalRecord);
});

const decodeGroupControl = Schema.decodeUnknownSync(task.WorkTaskManagerGroupControlSnapshot);
const decodeGroupRef = Schema.decodeUnknownSync(task.WorkTaskManagerGroupRef);
const decodeGroupPause = Schema.decodeUnknownSync(task.WorkTaskManagerGroupPauseInput);
const decodeGroupResume = Schema.decodeUnknownSync(task.WorkTaskManagerGroupResumeInput);
const decodeGroupControlResult = Schema.decodeUnknownSync(task.WorkTaskManagerGroupControlResult);

it("preserves explicit group control identities without inventing membership or runtime state", () => {
  const group = { environmentId: "local", groupId: "stable-group" };
  const empty = {
    group,
    controlRevision: 0,
    membershipRevision: 0,
    highestManagerGeneration: 0,
    currentManager: null,
    dispatchHold: "none",
  };
  expect(decodeGroupControl(empty)).toEqual(empty);
  const manager = { threadId: "actual-manager", managerGeneration: 1 };
  const current = { ...empty, highestManagerGeneration: 1, currentManager: manager };
  expect(decodeGroupControl(current)).toEqual(current);
  expect(decodeGroupControl({ ...current, currentManager: null }).highestManagerGeneration).toBe(1);
  for (const dispatchHold of ["none", "paused", "stopped"]) {
    expect(decodeGroupControl({ ...current, dispatchHold }).dispatchHold).toBe(dispatchHold);
  }
  expect(() => decodeGroupControl({ ...current, highestManagerGeneration: 2 })).toThrow();
  expect(() =>
    decodeGroupControl({ ...current, currentManager: { ...manager, managerGeneration: 0 } }),
  ).toThrow();
  expect(() =>
    decodeGroupControl({
      ...current,
      currentManager: { ...manager, managerGeneration: Number.MAX_SAFE_INTEGER + 1 },
    }),
  ).toThrow();
  for (const key of [
    "currentManager",
    "highestManagerGeneration",
    "dispatchHold",
    "controlRevision",
    "membershipRevision",
  ]) {
    const input: Record<string, unknown> = { ...empty };
    delete input[key];
    expect(() => decodeGroupControl(input)).toThrow();
  }
  for (const key of [
    "controlRevision",
    "membershipRevision",
    "highestManagerGeneration",
  ] as const) {
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Infinity]) {
      expect(() => decodeGroupControl({ ...empty, [key]: value })).toThrow();
    }
    expect(decodeGroupControl({ ...empty, [key]: Number.MAX_SAFE_INTEGER })[key]).toBe(
      Number.MAX_SAFE_INTEGER,
    );
  }
  for (const key of ["environmentId", "groupId"] as const) {
    expect(decodeGroupRef({ ...group, [key]: "😀".repeat(1_000) })[key]).toHaveLength(2_000);
    expect(() => decodeGroupRef({ ...group, [key]: "😀".repeat(1_001) })).toThrow();
    expect(() => decodeGroupRef({ ...group, [key]: " \t " })).toThrow();
  }
  expect(decodeGroupRef({ environmentId: " local ", groupId: " stable-group " })).toEqual(group);
  expect(() =>
    decodeGroupControl({ ...current, currentManager: { ...manager, threadId: "x".repeat(2_001) } }),
  ).toThrow();
  const guarded = {
    group,
    expectedControlRevision: 0,
    expectedMembershipRevision: 0,
    expectedManagerGeneration: null,
    reason: "user hold",
  };
  for (const decode of [decodeGroupPause, decodeGroupResume]) {
    expect(
      decode({ ...guarded, actor: "human", groupBudget: 999, ready: true, grant: "approve" }),
    ).toEqual(guarded);
    expect(decode({ ...guarded, expectedManagerGeneration: 1 }).expectedManagerGeneration).toBe(1);
    for (const key of [
      "expectedControlRevision",
      "expectedMembershipRevision",
      "expectedManagerGeneration",
    ]) {
      const input: Record<string, unknown> = { ...guarded };
      delete input[key];
      expect(() => decode(input)).toThrow();
      for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1])
        expect(() => decode({ ...guarded, [key]: value })).toThrow();
    }
    expect(() => decode({ ...guarded, expectedManagerGeneration: 0 })).toThrow();
    expect(() => decode({ ...guarded, reason: "\ufeff" })).toThrow();
    expect(() => decode({ ...guarded, reason: "x".repeat(2_001) })).toThrow();
  }
  expect(
    decodeGroupControlResult({ sequence: 0, control: current, ready: true, allowance: 99 }),
  ).toEqual({ sequence: 0, control: current });
  expect(() =>
    decodeGroupControlResult({ sequence: Number.MAX_SAFE_INTEGER + 1, control: current }),
  ).toThrow();
  expect(
    decodeGroupControl({
      ...current,
      running: true,
      memberships: [{ taskId: "a" }],
      caller: { actor: "human" },
    }),
  ).toEqual(current);
});

const membershipTask = { environmentId: "local", taskId: "a" };
const membershipGroup = { environmentId: "local", groupId: "group-a" };
const membershipOtherGroup = { environmentId: "local", groupId: "group-b" };
const membershipProvenance = {
  eventId: "joined-event",
  commandId: "joined-command",
  occurredAt: "2026-11-15T00:00:00.0001Z",
  performedBy: { subject: "user", sessionId: "session" },
};
const membershipCurrent = {
  state: "current",
  task: membershipTask,
  group: membershipGroup,
  membershipGeneration: 3,
  acceptedDefinitionRevisionAtJoin: 1,
  joinedMembershipRevision: 4,
  joined: membershipProvenance,
};
const membershipRevisions = {
  definitionRevision: 3,
  acceptedDefinitionRevision: 2,
  lifecycleRevision: 5,
  actionPolicyRevision: 6,
  completionPolicyRevision: 7,
};
const membershipSnapshot = {
  task: membershipTask,
  highestMembershipGeneration: 3,
  currentMembership: membershipCurrent,
  taskRevisions: membershipRevisions,
};
const membershipControl = {
  group: membershipGroup,
  controlRevision: 2,
  membershipRevision: 4,
  highestManagerGeneration: 1,
  currentManager: { threadId: "manager", managerGeneration: 1 },
  dispatchHold: "paused",
};
const decodeMembershipRecord = Schema.decodeUnknownSync(task.WorkTaskMembershipRecord);
const decodeMembershipSnapshot = Schema.decodeUnknownSync(task.WorkTaskMembershipSnapshot);
const decodeMemberReadInput = Schema.decodeUnknownSync(task.WorkTaskGroupMemberReadInput);
const decodeMemberRead = Schema.decodeUnknownSync(task.WorkTaskGroupMemberReadResult);
const decodeMembershipHistoryInput = Schema.decodeUnknownSync(task.WorkTaskMembershipHistoryInput);
const decodeMembershipHistory = Schema.decodeUnknownSync(task.WorkTaskMembershipHistoryResult);
const decodeMembershipChange = Schema.decodeUnknownSync(task.WorkTaskChangeMembershipInput);
const decodeMembershipChangeResult = Schema.decodeUnknownSync(task.WorkTaskChangeMembershipResult);

it("preserves membership join evidence and task-global generation across retirement and rejoin", () => {
  expect(decodeMembershipRecord(membershipCurrent)).toEqual(membershipCurrent);
  const retired = {
    ...membershipCurrent,
    state: "retired",
    retiredMembershipRevision: 5,
    retired: { ...membershipProvenance, occurredAt: "2026-11-15T00:00:00.0009Z" },
  };
  expect(decodeMembershipRecord(retired)).toEqual(retired);
  for (const occurredAt of ["2026-11-14T23:00:00.0001-01:00", "2026-11-15T00:00:00.0001Z"]) {
    expect(
      decodeMembershipRecord({ ...retired, retired: { ...retired.retired, occurredAt } }),
    ).toBeDefined();
  }
  for (const malformed of [
    { ...retired, retiredMembershipRevision: 4 },
    { ...retired, retired: { ...retired.retired, occurredAt: "2026-11-15T00:00:00.0000Z" } },
    { ...retired, retired: null },
    { ...membershipCurrent, group: { ...membershipGroup, environmentId: "other" } },
    { ...membershipCurrent, state: "candidate" },
  ])
    expect(() => decodeMembershipRecord(malformed)).toThrow();
  expect(decodeMembershipSnapshot(membershipSnapshot)).toEqual(membershipSnapshot);
  expect(
    decodeMembershipSnapshot({ ...membershipSnapshot, currentMembership: null })
      .highestMembershipGeneration,
  ).toBe(3);
  expect(
    decodeMembershipSnapshot({
      ...membershipSnapshot,
      highestMembershipGeneration: 0,
      currentMembership: null,
      taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null },
    }).currentMembership,
  ).toBeNull();
  for (const malformed of [
    {
      ...membershipSnapshot,
      currentMembership: null,
      taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null },
    },
    { ...membershipSnapshot, highestMembershipGeneration: 2 },
    { ...membershipSnapshot, task: { ...membershipTask, taskId: "b" } },
    { ...membershipSnapshot, currentMembership: retired },
    {
      ...membershipSnapshot,
      taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null },
    },
    {
      ...membershipSnapshot,
      taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: 4 },
    },
    {
      ...membershipSnapshot,
      currentMembership: { ...membershipCurrent, acceptedDefinitionRevisionAtJoin: 3 },
    },
  ])
    expect(() => decodeMembershipSnapshot(malformed)).toThrow();
  for (const key of [
    "membershipGeneration",
    "joinedMembershipRevision",
    "acceptedDefinitionRevisionAtJoin",
  ]) {
    for (const value of [0, -1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => decodeMembershipRecord({ ...membershipCurrent, [key]: value })).toThrow();
    }
  }
  for (const key of Object.keys(membershipRevisions)) {
    expect(() =>
      decodeMembershipSnapshot({
        ...membershipSnapshot,
        taskRevisions: { ...membershipRevisions, [key]: Number.MAX_SAFE_INTEGER + 1 },
      }),
    ).toThrow();
  }
  for (const key of ["currentMembership", "highestMembershipGeneration", "taskRevisions"]) {
    const missing: Record<string, unknown> = { ...membershipSnapshot };
    delete missing[key];
    expect(() => decodeMembershipSnapshot(missing)).toThrow();
  }
  for (const key of ["eventId", "commandId", "occurredAt"]) {
    expect(() =>
      decodeMembershipRecord({
        ...membershipCurrent,
        joined: { ...membershipProvenance, [key]: "x".repeat(2_001) },
      }),
    ).toThrow();
  }
  expect(() =>
    decodeMembershipRecord({
      ...membershipCurrent,
      joined: { ...membershipProvenance, occurredAt: "2026-02-30T00:00:00Z" },
    }),
  ).toThrow();
  for (const key of ["subject", "sessionId"]) {
    for (const value of ["\ufeff\t", "😀".repeat(1_001)]) {
      expect(() =>
        decodeMembershipRecord({
          ...membershipCurrent,
          joined: {
            ...membershipProvenance,
            performedBy: { ...membershipProvenance.performedBy, [key]: value },
          },
        }),
      ).toThrow();
    }
  }
  expect(
    decodeMembershipRecord({
      ...membershipCurrent,
      task: { ...membershipTask, taskId: "😀".repeat(1_000) },
    }).task.taskId,
  ).toHaveLength(2_000);
  expect(() =>
    decodeMembershipRecord({
      ...membershipCurrent,
      task: { ...membershipTask, taskId: "😀".repeat(1_001) },
    }),
  ).toThrow();
  expect(
    decodeMembershipRecord({
      ...membershipCurrent,
      task: { ...membershipTask, taskId: " a " },
      grant: true,
    }),
  ).toEqual(membershipCurrent);
});

it("recovers membership changes from bounded task and empty or populated group reads", () => {
  const input = { group: membershipGroup, limit: 100, pageToken: "opaque" };
  expect(decodeMemberReadInput(input)).toEqual(input);
  expect(decodeMemberReadInput({ group: membershipGroup })).toEqual({ group: membershipGroup });
  expect(decodeMembershipHistoryInput({ task: membershipTask, limit: 50 })).toEqual({
    task: membershipTask,
    limit: 50,
  });
  for (const limit of [0, 101, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => decodeMemberReadInput({ ...input, limit })).toThrow();
    expect(() => decodeMembershipHistoryInput({ task: membershipTask, limit })).toThrow();
  }
  expect(() => decodeMemberReadInput({ ...input, pageToken: "x".repeat(2_001) })).toThrow();
  const source = decodeMemberRead({
    sequence: 8,
    viewGeneration: 4,
    control: membershipControl,
    members: [membershipCurrent],
  });
  const destination = decodeMemberRead({
    sequence: 8,
    viewGeneration: 4,
    control: { ...membershipControl, group: membershipOtherGroup },
    members: [],
  });
  const history = decodeMembershipHistory({
    sequence: 8,
    viewGeneration: 4,
    snapshot: membershipSnapshot,
    memberships: [membershipCurrent],
  });
  const guard = (control: typeof source.control) => ({
    group: control.group,
    expectedControlRevision: control.controlRevision,
    expectedMembershipRevision: control.membershipRevision,
    expectedManagerGeneration: control.currentManager?.managerGeneration ?? null,
  });
  const change = {
    task: history.snapshot.task,
    expectedTaskRevisions: history.snapshot.taskRevisions,
    expectedMembershipGeneration: history.snapshot.highestMembershipGeneration,
    sourceGroup: guard(source.control),
    destinationGroup: guard(destination.control),
    reason: "move selected task",
  };
  expect(
    decodeMembershipChange({
      ...change,
      actor: "human",
      allowance: 500,
      grant: "allow",
      ready: true,
    }),
  ).toEqual(change);
  expect(
    decodeMembershipChange({
      ...change,
      destinationGroup: null,
      expectedTaskRevisions: { ...change.expectedTaskRevisions, acceptedDefinitionRevision: null },
    }).destinationGroup,
  ).toBeNull();
  expect(
    decodeMembershipChange({ ...change, sourceGroup: null }).expectedMembershipGeneration,
  ).toBe(3);
  for (const malformed of [
    { ...change, sourceGroup: null, destinationGroup: null },
    { ...change, destinationGroup: change.sourceGroup },
    { ...change, expectedMembershipGeneration: 0 },
    {
      ...change,
      destinationGroup: {
        ...change.destinationGroup,
        group: { ...membershipOtherGroup, environmentId: "foreign" },
      },
    },
    {
      ...change,
      expectedTaskRevisions: { ...change.expectedTaskRevisions, acceptedDefinitionRevision: null },
    },
    { ...change, reason: "\t\ufeff" },
  ])
    expect(() => decodeMembershipChange(malformed)).toThrow();
  for (const key of [
    "sourceGroup",
    "destinationGroup",
    "expectedTaskRevisions",
    "expectedMembershipGeneration",
  ]) {
    const missing: Record<string, unknown> = { ...change };
    delete missing[key];
    expect(() => decodeMembershipChange(missing)).toThrow();
  }
  for (const key of [
    "expectedControlRevision",
    "expectedMembershipRevision",
    "expectedManagerGeneration",
  ]) {
    const missing: Record<string, unknown> = { ...change.sourceGroup };
    delete missing[key];
    expect(() => decodeMembershipChange({ ...change, sourceGroup: missing })).toThrow();
  }
  for (const count of [0, 100]) {
    const members = Array.from({ length: count }, (_, index) => ({
      ...membershipCurrent,
      task: { ...membershipTask, taskId: `task-${index}` },
    }));
    expect(decodeMemberRead({ ...source, members }).members).toHaveLength(count);
  }
  for (const members of [
    Array.from({ length: 101 }, (_, index) => ({
      ...membershipCurrent,
      task: { ...membershipTask, taskId: `task-${index}` },
    })),
    [membershipCurrent, membershipCurrent],
    [{ ...membershipCurrent, group: membershipOtherGroup }],
    [{ ...membershipCurrent, joinedMembershipRevision: 5 }],
  ])
    expect(() => decodeMemberRead({ ...source, members })).toThrow();
  for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => decodeMemberRead({ ...source, sequence: value })).toThrow();
    expect(() => decodeMembershipHistory({ ...history, viewGeneration: value })).toThrow();
  }
});

it("rejects contradictory membership history and mutation-result snapshots", () => {
  const retired = {
    ...membershipCurrent,
    state: "retired",
    retiredMembershipRevision: 5,
    retired: membershipProvenance,
  };
  const historical = { ...retired, membershipGeneration: 2 };
  const page = {
    sequence: 8,
    viewGeneration: 4,
    snapshot: membershipSnapshot,
    memberships: [membershipCurrent, historical],
  };
  expect(decodeMembershipHistory(page).memberships).toHaveLength(2);
  expect(decodeMembershipHistory({ ...page, memberships: [historical] }).memberships).toHaveLength(
    1,
  );
  for (const memberships of [
    [retired],
    [historical, membershipCurrent],
    [membershipCurrent, membershipCurrent],
    [{ ...membershipCurrent, joined: { ...membershipProvenance, commandId: "other-command" } }],
    [{ ...historical, task: { ...membershipTask, taskId: "other" } }],
    [{ ...historical, membershipGeneration: 4 }],
  ])
    expect(() => decodeMembershipHistory({ ...page, memberships })).toThrow();
  expect(() =>
    decodeMembershipHistory({
      ...page,
      snapshot: { ...membershipSnapshot, currentMembership: null },
      memberships: [membershipCurrent],
    }),
  ).toThrow();
  const removed = { ...membershipSnapshot, currentMembership: null };
  expect(
    decodeMembershipHistory({ ...page, snapshot: removed, memberships: [retired, historical] })
      .snapshot.highestMembershipGeneration,
  ).toBe(3);
  expect(() =>
    decodeMembershipHistory({
      ...page,
      snapshot: {
        ...removed,
        taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null },
      },
      memberships: [retired],
    }),
  ).toThrow();
  const boundedHistory = Array.from({ length: 100 }, (_, index) => ({
    ...historical,
    membershipGeneration: 100 - index,
  }));
  const historicalSnapshot = { ...removed, highestMembershipGeneration: 100 };
  expect(
    decodeMembershipHistory({ ...page, snapshot: historicalSnapshot, memberships: boundedHistory })
      .memberships,
  ).toHaveLength(100);
  expect(() =>
    decodeMembershipHistory({
      ...page,
      snapshot: { ...historicalSnapshot, highestMembershipGeneration: 101 },
      memberships: [{ ...historical, membershipGeneration: 101 }, ...boundedHistory],
    }),
  ).toThrow();
  for (const highestMembershipGeneration of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => decodeMembershipSnapshot({ ...removed, highestMembershipGeneration })).toThrow();
  }
  const movedSnapshot = {
    ...membershipSnapshot,
    highestMembershipGeneration: 4,
    currentMembership: {
      ...membershipCurrent,
      group: membershipOtherGroup,
      membershipGeneration: 4,
      joinedMembershipRevision: 5,
    },
  };
  expect(
    decodeMembershipChangeResult({
      sequence: 9,
      snapshot: movedSnapshot,
      controls: [
        { ...membershipControl, membershipRevision: 5 },
        {
          ...membershipControl,
          group: membershipOtherGroup,
          membershipRevision: 5,
          currentManager: { threadId: "other-manager", managerGeneration: 1 },
        },
      ],
    }).snapshot.highestMembershipGeneration,
  ).toBe(4);
  const result = { sequence: 9, snapshot: membershipSnapshot, controls: [membershipControl] };
  expect(decodeMembershipChangeResult(result)).toEqual(result);
  expect(decodeMembershipChangeResult({ ...result, snapshot: removed })).toEqual({
    ...result,
    snapshot: removed,
  });
  for (const controls of [
    [],
    [membershipControl, membershipControl],
    [{ ...membershipControl, group: membershipOtherGroup }],
    [{ ...membershipControl, membershipRevision: 3 }],
    [{ ...membershipControl, group: { ...membershipGroup, environmentId: "foreign" } }],
  ])
    expect(() => decodeMembershipChangeResult({ ...result, controls })).toThrow();
});

const workBinding = {
  version: 2,
  workClaimId: "work-claim",
  task: membershipTask,
  group: membershipGroup,
  manager: { threadId: "manager", managerGeneration: 2 },
  membershipGeneration: 3,
  membershipRevision: 4,
  controlRevision: 2,
  graphRevision: 1,
  projectId: "launch-project",
  providerInstanceId: "codex",
  taskRevisions: membershipRevisions,
  commandId: "accept-command",
  runId: "accepted-run",
  messageId: "accepted-message",
  effectId: "accepted-effect",
};
const initialWorkOrigin = {
  type: "initial_start",
  startCommandId: "original-start-command",
  startEventId: "original-start-event",
  startSequence: 2,
};
const acceptedWork = {
  binding: workBinding,
  acceptedEventId: "accepted-event",
  acceptedSequence: 10,
  acceptedAt: "2026-11-15T00:00:00.0001Z",
  sourceEventId: "ready-event",
  sourceSequence: 8,
  observedWorkSequence: 7,
  prerequisites: [],
  launchOrigin: initialWorkOrigin,
};
const decodeWorkBinding = Schema.decodeUnknownSync(task.WorkTaskRunBindingV2);
const decodeAcceptedWork = Schema.decodeUnknownSync(task.WorkTaskAcceptedWorkClaim);

it("records bounded selected work without treating a claim as caller or provider origin", () => {
  expect(decodeAcceptedWork(acceptedWork)).toEqual(acceptedWork);
  expect(decodeWorkBinding(workBinding)).toEqual(workBinding);
  expect(decodeWorkBinding({ ...workBinding, graphRevision: 0 }).graphRevision).toBe(0);
  expect(decodeAcceptedWork({ ...acceptedWork, sourceSequence: 9 }).observedWorkSequence).toBe(7);
  for (const key of Object.keys(workBinding)) {
    const missing: Record<string, unknown> = { ...workBinding };
    delete missing[key];
    expect(() => decodeWorkBinding(missing)).toThrow();
  }
  for (const key of Object.keys(acceptedWork)) {
    const missing: Record<string, unknown> = { ...acceptedWork };
    delete missing[key];
    expect(() => decodeAcceptedWork(missing)).toThrow();
  }
  for (const key of ["workClaimId", "projectId", "commandId", "runId", "messageId", "effectId"]) {
    for (const value of ["\t\ufeff", "😀".repeat(1_001)]) {
      expect(() => decodeWorkBinding({ ...workBinding, [key]: value })).toThrow();
    }
    expect(decodeWorkBinding({ ...workBinding, [key]: "😀".repeat(1_000) })).toBeDefined();
  }
  for (const providerInstanceId of ["a/b", "x".repeat(65), "1-codex"]) {
    expect(() => decodeWorkBinding({ ...workBinding, providerInstanceId })).toThrow();
  }
  expect(
    decodeWorkBinding({ ...workBinding, providerInstanceId: " Codex_2 " }).providerInstanceId,
  ).toBe("Codex_2");
  for (const malformed of [
    { ...workBinding, version: 1 },
    { ...workBinding, group: { ...membershipGroup, environmentId: "foreign" } },
    { ...workBinding, membershipGeneration: 0 },
    { ...workBinding, membershipRevision: 0 },
    { ...workBinding, manager: { ...workBinding.manager, managerGeneration: 0 } },
    { ...workBinding, taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null } },
    { ...workBinding, taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: 4 } },
  ])
    expect(() => decodeWorkBinding(malformed)).toThrow();
  for (const key of [
    "membershipGeneration",
    "membershipRevision",
    "controlRevision",
    "graphRevision",
  ]) {
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => decodeWorkBinding({ ...workBinding, [key]: value })).toThrow();
    }
  }
  for (const key of ["acceptedSequence", "sourceSequence", "observedWorkSequence"]) {
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => decodeAcceptedWork({ ...acceptedWork, [key]: value })).toThrow();
    }
  }
  expect(() => decodeAcceptedWork({ ...acceptedWork, sourceSequence: 11 })).toThrow();
  expect(() => decodeAcceptedWork({ ...acceptedWork, observedWorkSequence: 11 })).toThrow();
  for (const acceptedAt of [
    "not-a-time",
    "2026-02-30T00:00:00Z",
    `2026-11-15T00:00:00.${"0".repeat(2_000)}Z`,
  ]) {
    expect(() => decodeAcceptedWork({ ...acceptedWork, acceptedAt })).toThrow();
  }
  for (const key of ["acceptedEventId", "sourceEventId"]) {
    expect(() => decodeAcceptedWork({ ...acceptedWork, [key]: "x".repeat(2_001) })).toThrow();
  }
  expect(
    decodeAcceptedWork({
      ...acceptedWork,
      actor: "human",
      grant: "allow",
      emittingRunId: "invented-origin",
      providerSessionId: "credential-namespace",
      binding: {
        ...workBinding,
        emittingRunId: "invented-origin",
        providerSessionId: "namespace",
        prerequisites: [{ secret: "not-a-run-payload" }],
      },
    }),
  ).toEqual(acceptedWork);
  const witnesses = Array.from({ length: 64 }, (_, index) => ({
    edgeId: `edge-${index}`,
    blocker: { environmentId: "local", taskId: `blocker-${index}` },
    completionEventId: `completed-${index}`,
    completionSequence: 6,
  }));
  expect(
    decodeAcceptedWork({ ...acceptedWork, prerequisites: witnesses }).prerequisites,
  ).toHaveLength(64);
  for (const prerequisites of [
    [
      ...witnesses,
      { ...witnesses[0], edgeId: "edge-extra", blocker: { ...membershipTask, taskId: "extra" } },
    ],
    [witnesses[0], witnesses[0]],
    [witnesses[0], { ...witnesses[0], edgeId: "another-edge" }],
    [witnesses[0], { ...witnesses[1], edgeId: witnesses[0]!.edgeId }],
    [{ ...witnesses[0], blocker: membershipTask }],
    [{ ...witnesses[0], blocker: { ...membershipTask, environmentId: "foreign" } }],
    [{ ...witnesses[0], completionSequence: 11 }],
    [{ ...witnesses[0], completionEventId: "x".repeat(2_001) }],
  ])
    expect(() => decodeAcceptedWork({ ...acceptedWork, prerequisites })).toThrow();
});

it("distinguishes original Start from one reserved automatic follow-up unit", () => {
  const initial = decodeAcceptedWork(acceptedWork);
  expect(initial.binding.commandId).not.toBe(initialWorkOrigin.startCommandId);
  expect(initial.launchOrigin).not.toHaveProperty("allowanceUnit");
  const automatic = {
    type: "automatic_followup",
    reservationId: "reservation",
    reservationCommandId: "reserve-command",
    allowanceUnit: 1,
    observedDirtyGeneration: 6,
    acknowledgedGeneration: 5,
  };
  expect(decodeAcceptedWork({ ...acceptedWork, launchOrigin: automatic }).launchOrigin).toEqual(
    automatic,
  );
  for (const acknowledgedGeneration of [0, 6]) {
    expect(
      decodeAcceptedWork({
        ...acceptedWork,
        launchOrigin: { ...automatic, acknowledgedGeneration },
      }),
    ).toBeDefined();
  }
  for (const key of [
    "reservationId",
    "reservationCommandId",
    "allowanceUnit",
    "observedDirtyGeneration",
    "acknowledgedGeneration",
  ]) {
    const missing: Record<string, unknown> = { ...automatic };
    delete missing[key];
    expect(() => decodeAcceptedWork({ ...acceptedWork, launchOrigin: missing })).toThrow();
    expect(() =>
      decodeAcceptedWork({
        ...acceptedWork,
        launchOrigin: { ...initialWorkOrigin, [key]: automatic[key as keyof typeof automatic] },
      }),
    ).toThrow();
  }
  for (const key of ["startCommandId", "startEventId", "startSequence"]) {
    const missing: Record<string, unknown> = { ...initialWorkOrigin };
    delete missing[key];
    expect(() => decodeAcceptedWork({ ...acceptedWork, launchOrigin: missing })).toThrow();
    expect(() =>
      decodeAcceptedWork({
        ...acceptedWork,
        launchOrigin: {
          ...automatic,
          [key]: initialWorkOrigin[key as keyof typeof initialWorkOrigin],
        },
      }),
    ).toThrow();
  }
  for (const launchOrigin of [
    { ...initialWorkOrigin, startSequence: 11 },
    { ...initialWorkOrigin, startCommandId: "x".repeat(2_001) },
    { ...automatic, acknowledgedGeneration: 7 },
    { ...automatic, allowanceUnit: 0 },
    { ...automatic, observedDirtyGeneration: 0 },
    { ...automatic, reservationId: "\t" },
    { ...automatic, allowanceUnit: Number.MAX_SAFE_INTEGER + 1 },
  ])
    expect(() => decodeAcceptedWork({ ...acceptedWork, launchOrigin })).toThrow();
});

const decodeOldTaskEventForGraph = Schema.decodeUnknownSync(task.WorkTaskEvent);

describe("staged graph event family", () => {
  const occurredAt = "2026-11-15T00:00:00.0001Z";
  const provenance = {
    eventId: "adopt",
    commandId: "command",
    occurredAt,
    adoptedBy: { subject: "user", sessionId: "session" },
  };
  const edge = {
    edgeId: "edge",
    dependent: { environmentId: "env", taskId: "dependent" },
    blocker: { environmentId: "env", taskId: "blocker" },
    createdGraphRevision: 4,
    provenance,
  };
  const adoption = {
    type: "task.graph-edge-adopted",
    id: "adopt",
    environmentId: "env",
    occurredAt,
    commandId: "command",
    previousGraphRevision: 3,
    graphRevision: 4,
    edge,
  };
  const removal = {
    ...adoption,
    type: "task.graph-edge-removed",
    id: "remove",
    previousGraphRevision: 5,
    graphRevision: 6,
    provenance: { ...provenance, eventId: "remove" },
  };
  const proposal = {
    type: "task.graph-proposal-recorded",
    id: "propose",
    environmentId: "env",
    occurredAt,
    commandId: "command",
    graphRevision: 8,
    proposal: {
      proposalId: "proposal",
      dependent: edge.dependent,
      blocker: { environmentId: "foreign", taskId: "blocker" },
      observedGraphRevision: 7,
      proposedAt: occurredAt,
      proposedBy: { type: "user", ...provenance.adoptedBy },
    },
  };

  it("records local adoption/removal and inert foreign proposals without activating old task events", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskGraphEvent);
    for (const event of [adoption, removal, proposal]) {
      expect(decode(event)).toEqual(event);
      expect(() => decodeOldTaskEventForGraph(event)).toThrow();
      expect(decode({ ...event, actor: "human", grant: "all", authorized: true })).toEqual(event);
    }
    expect(decode(removal)).toMatchObject({
      edge: { provenance },
      provenance: { ...provenance, eventId: "remove" },
    });
    for (const patch of [
      { graphRevision: 3 },
      { graphRevision: 5 },
      { previousGraphRevision: 4 },
      { previousGraphRevision: 5 },
      { graphRevision: Number.MAX_SAFE_INTEGER + 1 },
      { previousGraphRevision: Number.MAX_SAFE_INTEGER, graphRevision: Number.MAX_SAFE_INTEGER },
      { edge: { ...edge, createdGraphRevision: 3 } },
      { edge: { ...edge, blocker: { ...edge.blocker, environmentId: "foreign" } } },
      { environmentId: "foreign" },
      { id: "other" },
      { commandId: "other" },
      { occurredAt: "2026-11-15T00:00:00.0009Z" },
      { commandId: null },
      { id: "x".repeat(2_001) },
      { environmentId: "x".repeat(2_001) },
      { commandId: "x".repeat(2_001) },
      { occurredAt: "not-a-time" },
      { previousGraphRevision: -1, graphRevision: 0 },
      { previousGraphRevision: 3.5, graphRevision: 4.5 },
    ])
      expect(() => decode({ ...adoption, ...patch })).toThrow();
    for (const patch of [
      { previousGraphRevision: 3, graphRevision: 4 },
      { id: "adopt", provenance },
      { provenance: { ...provenance, eventId: "different" } },
      { provenance: { ...removal.provenance, commandId: "different" } },
      { provenance: { ...removal.provenance, occurredAt: "2026-11-15T00:00:00.0009Z" } },
    ])
      expect(() => decode({ ...removal, ...patch })).toThrow();
    for (const patch of [
      { graphRevision: 6 },
      { environmentId: "foreign" },
      { proposal: { ...proposal.proposal, proposedAt: "2026-11-15T00:00:00.0009Z" } },
      { proposal: { ...proposal.proposal, observedGraphRevision: Number.MAX_SAFE_INTEGER + 1 } },
    ])
      expect(() => decode({ ...proposal, ...patch })).toThrow();
    expect(() => decode({ ...adoption, type: "task.graph-revision-bumped" })).toThrow();
  });

  it("binds exact persisted command/event/time independently of topology revision", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskPersistedGraphEvent);
    for (const event of [adoption, removal, proposal]) {
      const context = { sequence: 100, eventId: event.id, occurredAt, commandId: "command" };
      const envelope = { event, context };
      expect(decode(envelope)).toEqual(envelope);
      expect(decode({ ...envelope, context: { ...context, sequence: 0 } })).toBeDefined();
      for (const patch of [
        { sequence: -1 },
        { sequence: 1.5 },
        { sequence: Number.MAX_SAFE_INTEGER + 1 },
        { eventId: "different" },
        { commandId: "different" },
        { commandId: null },
        { eventId: "x".repeat(2_001) },
        { commandId: "x".repeat(2_001) },
        { occurredAt: "2026-11-15T00:00:00.0009Z" },
        { occurredAt: "2026-11-15T00:00:00.000100Z" },
      ])
        expect(() => decode({ ...envelope, context: { ...context, ...patch } })).toThrow();
    }
  });
});

describe("staged group control and membership events", () => {
  const occurredAt = "2026-11-15T00:00:01.0001Z";
  const performedBy = { subject: "user", sessionId: "session" };
  const group = { environmentId: "env", groupId: "a" };
  const otherGroup = { environmentId: "env", groupId: "b" };
  const control = {
    group,
    controlRevision: 0,
    membershipRevision: 0,
    highestManagerGeneration: 0,
    currentManager: null,
    dispatchHold: "none",
  };
  const common = {
    id: "create",
    environmentId: "env",
    occurredAt,
    commandId: "command",
    performedBy,
  };
  const created = { ...common, type: "task-group.created", control };
  const bound = {
    ...control,
    controlRevision: 1,
    highestManagerGeneration: 1,
    currentManager: { threadId: "manager", managerGeneration: 1 },
  };
  const managerChanged = {
    ...common,
    id: "bind",
    type: "task-group.manager-changed",
    previousControl: control,
    control: bound,
  };
  const taskRef = { environmentId: "env", taskId: "task" };
  const taskRevisions = {
    definitionRevision: 2,
    acceptedDefinitionRevision: 1,
    lifecycleRevision: 2,
    actionPolicyRevision: 3,
    completionPolicyRevision: 4,
  };
  const detached = {
    task: taskRef,
    highestMembershipGeneration: 0,
    currentMembership: null,
    taskRevisions,
  };
  const joined = {
    task: taskRef,
    group,
    membershipGeneration: 1,
    acceptedDefinitionRevisionAtJoin: 1,
    joinedMembershipRevision: 1,
    joined: { eventId: "join", commandId: "command", occurredAt, performedBy },
    state: "current",
  };
  const joinedSnapshot = { ...detached, highestMembershipGeneration: 1, currentMembership: joined };
  const groupAfterJoin = { ...bound, membershipRevision: 1 };
  const join = {
    ...common,
    id: "join",
    type: "task.membership-changed",
    previousSnapshot: detached,
    snapshot: joinedSnapshot,
    retiredMembership: null,
    groupTransitions: [
      { companionEventId: "join-group", previousControl: bound, control: groupAfterJoin },
    ],
  };
  const movedAt = "2026-11-15T00:00:02.0001Z";
  const moveProvenance = {
    eventId: "move",
    commandId: "command",
    occurredAt: movedAt,
    performedBy,
  };
  const otherControl = { ...bound, group: otherGroup };
  const moved = {
    ...joined,
    group: otherGroup,
    membershipGeneration: 2,
    joined: moveProvenance,
  };
  const retired = {
    ...joined,
    state: "retired",
    retiredMembershipRevision: 2,
    retired: moveProvenance,
  };
  const move = {
    ...join,
    id: "move",
    occurredAt: movedAt,
    previousSnapshot: joinedSnapshot,
    snapshot: { ...joinedSnapshot, highestMembershipGeneration: 2, currentMembership: moved },
    retiredMembership: retired,
    groupTransitions: [
      {
        companionEventId: "move-a",
        previousControl: groupAfterJoin,
        control: { ...groupAfterJoin, membershipRevision: 2 },
      },
      {
        companionEventId: "move-b",
        previousControl: otherControl,
        control: { ...otherControl, membershipRevision: 1 },
      },
    ],
  };
  const companionsFor = (event: typeof join | typeof move) =>
    event.groupTransitions.map((transition) => ({
      ...common,
      id: transition.companionEventId,
      occurredAt: event.occurredAt,
      type: "task-group.membership-changed",
      rootEventId: event.id,
      task: taskRef,
      previousControl: transition.previousControl,
      control: transition.control,
    }));

  it("records independent group control intent without implying quiescence or clearing work", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskGroupControlEvent);
    expect(decode(created)).toEqual(created);
    expect(decode(managerChanged)).toEqual(managerChanged);
    const retiredManager = { ...bound, controlRevision: 2, currentManager: null };
    expect(
      decode({ ...managerChanged, previousControl: bound, control: retiredManager }),
    ).toBeDefined();
    expect(
      decode({
        ...managerChanged,
        previousControl: retiredManager,
        control: {
          ...bound,
          controlRevision: 3,
          highestManagerGeneration: 2,
          currentManager: { threadId: "next", managerGeneration: 2 },
        },
      }),
    ).toBeDefined();
    for (const hold of ["paused", "stopped"]) {
      const held = { ...bound, controlRevision: 2, dispatchHold: hold };
      const event = {
        ...common,
        type: "task-group.hold-changed",
        previousControl: bound,
        control: held,
      };
      expect(decode(event)).toEqual(event);
      expect(
        decode({ ...event, previousControl: held, control: { ...bound, controlRevision: 3 } }),
      ).toBeDefined();
    }
    for (const patch of [
      { control: bound },
      { control: { ...control, membershipRevision: 1 } },
      { control: { ...control, dispatchHold: "paused" } },
      { environmentId: "foreign" },
      { commandId: null },
      { id: "x".repeat(2_001) },
    ])
      expect(() => decode({ ...created, ...patch })).toThrow();
    for (const changed of [
      { ...bound, controlRevision: 2 },
      { ...bound, membershipRevision: 1 },
      { ...bound, dispatchHold: "paused" },
      { ...bound, group: otherGroup },
      {
        ...bound,
        highestManagerGeneration: 2,
        currentManager: { threadId: "manager", managerGeneration: 2 },
      },
    ])
      expect(() => decode({ ...managerChanged, control: changed })).toThrow();
    expect(() =>
      decode({
        ...managerChanged,
        previousControl: bound,
        control: {
          ...bound,
          controlRevision: 2,
          highestManagerGeneration: 2,
          currentManager: { threadId: "manager", managerGeneration: 2 },
        },
      }),
    ).toThrow();
    expect(() =>
      decode({
        ...managerChanged,
        previousControl: { ...control, controlRevision: Number.MAX_SAFE_INTEGER },
        control: { ...bound, controlRevision: Number.MAX_SAFE_INTEGER },
      }),
    ).toThrow();
    for (const changed of [
      bound,
      { ...bound, controlRevision: 2, dispatchHold: "paused", membershipRevision: 1 },
      { ...bound, controlRevision: 2, dispatchHold: "paused", currentManager: null },
    ])
      expect(() =>
        decode({
          ...common,
          type: "task-group.hold-changed",
          previousControl: bound,
          control: changed,
        }),
      ).toThrow();
    expect(decode({ ...created, actor: "human", approved: true, clearActiveClaim: true })).toEqual(
      created,
    );
  });

  it("preserves task-local membership lineage across join, move, removal and rejoin", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskMembershipChangedEvent);
    const decodeBatch = Schema.decodeUnknownSync(task.WorkTaskMembershipEventBatch);
    for (const event of [join, move]) {
      expect(decode(event)).toEqual(event);
      expect(decodeBatch({ event, companions: companionsFor(event) })).toBeDefined();
    }
    const removedAt = "2026-11-15T00:00:03Z";
    const remove = {
      ...move,
      id: "remove",
      occurredAt: removedAt,
      previousSnapshot: move.snapshot,
      snapshot: { ...move.snapshot, currentMembership: null },
      retiredMembership: {
        ...moved,
        state: "retired",
        retiredMembershipRevision: 2,
        retired: { eventId: "remove", commandId: "command", occurredAt: removedAt, performedBy },
      },
      groupTransitions: [
        {
          companionEventId: "remove-b",
          previousControl: move.groupTransitions[1]!.control,
          control: { ...otherControl, membershipRevision: 2 },
        },
      ],
    };
    expect(decode(remove)).toEqual(remove);
    const rejoin = {
      ...join,
      id: "rejoin",
      previousSnapshot: remove.snapshot,
      snapshot: {
        ...joinedSnapshot,
        highestMembershipGeneration: 3,
        currentMembership: {
          ...joined,
          membershipGeneration: 3,
          joinedMembershipRevision: 3,
          joined: { ...joined.joined, eventId: "rejoin" },
        },
      },
      groupTransitions: [
        {
          ...join.groupTransitions[0]!,
          previousControl: { ...bound, membershipRevision: 2 },
          control: { ...bound, membershipRevision: 3 },
        },
      ],
    };
    expect(decode(rejoin)).toEqual(rejoin);
    for (const patch of [
      {
        snapshot: {
          ...joinedSnapshot,
          highestMembershipGeneration: 2,
          currentMembership: { ...joined, membershipGeneration: 2 },
        },
      },
      {
        snapshot: {
          ...joinedSnapshot,
          currentMembership: { ...joined, joinedMembershipRevision: 2 },
        },
      },
      {
        snapshot: {
          ...joinedSnapshot,
          currentMembership: { ...joined, joined: { ...joined.joined, eventId: "wrong" } },
        },
      },
      {
        snapshot: { ...joinedSnapshot, taskRevisions: { ...taskRevisions, lifecycleRevision: 3 } },
      },
      {
        previousSnapshot: {
          ...detached,
          taskRevisions: { ...taskRevisions, acceptedDefinitionRevision: null },
        },
      },
      { retiredMembership: retired },
      { groupTransitions: [] },
      { groupTransitions: [...join.groupTransitions, ...join.groupTransitions] },
      { groupTransitions: [{ ...join.groupTransitions[0]!, companionEventId: "join" }] },
      { environmentId: "foreign" },
    ])
      expect(() => decode({ ...join, ...patch })).toThrow();
    for (const patch of [
      { retiredMembership: null },
      { retiredMembership: { ...retired, joined: moveProvenance } },
      { retiredMembership: { ...retired, retired: { ...moveProvenance, commandId: "other" } } },
      { groupTransitions: [move.groupTransitions[1]] },
      { snapshot: joinedSnapshot },
      {
        snapshot: {
          ...move.snapshot,
          highestMembershipGeneration: 3,
          currentMembership: { ...moved, membershipGeneration: 3 },
        },
      },
    ])
      expect(() => decode({ ...move, ...patch })).toThrow();
    expect(() =>
      decode({
        ...move,
        id: joined.joined.eventId,
        snapshot: {
          ...move.snapshot,
          currentMembership: {
            ...moved,
            joined: { ...moveProvenance, eventId: joined.joined.eventId },
          },
        },
        retiredMembership: {
          ...retired,
          retired: { ...moveProvenance, eventId: joined.joined.eventId },
        },
      }),
    ).toThrow();
    expect(() =>
      decode({
        ...move,
        groupTransitions: move.groupTransitions.map((transition, index) =>
          index === 0 ? { ...transition, companionEventId: joined.joined.eventId } : transition,
        ),
      }),
    ).toThrow();
    const companions = companionsFor(move);
    for (const bad of [
      [],
      [companions[0]],
      [companions[0], companions[0]],
      [companions[0], { ...companions[1], rootEventId: "other" }],
      [companions[0], { ...companions[1], commandId: "other" }],
      [companions[0], { ...companions[1], performedBy: { ...performedBy, subject: "other" } }],
      [companions[0], { ...companions[1], control: { ...otherControl, membershipRevision: 2 } }],
    ])
      expect(() => decodeBatch({ event: move, companions: bad })).toThrow();
  });

  it("binds group and task-owned persisted envelopes exactly without altering old event unions", () => {
    const decodeControl = Schema.decodeUnknownSync(task.WorkTaskPersistedGroupControlEvent);
    const decodeMembership = Schema.decodeUnknownSync(task.WorkTaskPersistedMembershipEvent);
    const context = { sequence: 100, eventId: created.id, occurredAt, commandId: "command" };
    expect(decodeControl({ event: created, context })).toBeDefined();
    expect(
      decodeMembership({ event: join, context: { ...context, eventId: join.id } }),
    ).toBeDefined();
    for (const patch of [
      { eventId: "wrong" },
      { commandId: null },
      { commandId: "wrong" },
      { occurredAt: "2026-11-15T00:00:01.0009Z" },
      { sequence: -1 },
      { sequence: Number.MAX_SAFE_INTEGER + 1 },
    ])
      expect(() => decodeControl({ event: created, context: { ...context, ...patch } })).toThrow();
    for (const event of [created, managerChanged, join, move])
      expect(() => decodeOldTaskEventForGraph(event)).toThrow();
  });
});

describe("staged selected work and reconciliation facts", () => {
  const common = {
    id: "observation",
    environmentId: membershipTask.environmentId,
    commandId: "reconcile-command",
    occurredAt: "2026-11-15T00:00:00.0009Z",
    attribution: { type: "server" },
  };
  const accepted = {
    ...common,
    type: "task-group.work-accepted",
    id: acceptedWork.acceptedEventId,
    commandId: workBinding.commandId,
    occurredAt: acceptedWork.acceptedAt,
    previousActiveClaimId: null,
    claim: acceptedWork,
    control: {
      ...membershipControl,
      highestManagerGeneration: 2,
      currentManager: workBinding.manager,
      dispatchHold: "none",
    },
    membership: membershipSnapshot,
  };
  const claim = {
    binding: workBinding,
    acceptedEventId: acceptedWork.acceptedEventId,
    acceptedSequence: 10,
    acceptedAt: acceptedWork.acceptedAt,
  };
  const uncertain = {
    ...common,
    id: "uncertain",
    type: "task-group.work-uncertain",
    claim,
    reason: "Unresponsive launch",
    sourceEventId: "unresponsive-event",
    sourceSequence: 11,
  };
  const terminalRunEvent = { type: "run.updated", eventId: "terminal-run", sequence: 30 };
  const release = {
    ...common,
    id: "release",
    type: "task-group.work-released",
    claim,
    observedSequence: 35,
    releaseEvidence: {
      type: "not_started",
      terminalRunEvent,
      settlementCommandId: "cancel-command",
    },
  };
  const settledEvidence = {
    type: "run_settled",
    terminalRunEvent,
    attempt: {
      attemptId: "attempt",
      providerThreadId: "provider-thread",
      providerTurnId: null,
      terminalAttemptEvent: {
        type: "run-attempt.updated",
        eventId: "terminal-attempt",
        sequence: 25,
      },
    },
    interruptResult: {
      type: "turn-item.updated",
      turnItemId: "interrupt-result",
      eventId: "interrupt-event",
      sequence: 26,
    },
  };
  const envelope = <E extends { id: string; commandId: string; occurredAt: string }>(
    event: E,
    sequence: number,
  ) => ({
    event,
    context: {
      sequence,
      eventId: event.id,
      commandId: event.commandId,
      occurredAt: event.occurredAt,
    },
  });
  const membership = { group: membershipGroup, membershipGeneration: 3 };
  const dirty = {
    ...common,
    type: "task.dirty-source-recorded",
    task: membershipTask,
    membership,
    sourceEventId: "source",
    sourceSequence: 8,
    previousDirtyGeneration: 3,
    dirtyGeneration: 4,
    acknowledgedGeneration: 2,
  };
  const ack = {
    ...common,
    type: "task.dirty-acknowledged",
    task: membershipTask,
    membership,
    sourceEventId: "observed-source",
    sourceSequence: 9,
    previousAcknowledgedGeneration: 2,
    acknowledgedGeneration: 4,
    observedDirtyGeneration: 4,
    dirtyGeneration: 5,
  };
  const basis = {
    taskRevisions: membershipRevisions,
    graphRevision: 1,
    controlRevision: 2,
    membershipRevision: 4,
    managerGeneration: 2,
  };
  const ready = {
    ...common,
    type: "task.ready-sequence-updated",
    task: membershipTask,
    ...membership,
    basis,
    sourceEventId: "ready-source",
    sourceSequence: 8,
    kind: "qualified",
    previousOldestReadySequence: null,
    oldestReadySequence: 9,
  };
  const graphScan = {
    ...common,
    type: "task.reconciliation-progressed",
    scan: {
      kind: "graph_dependents",
      blocker: membershipTask,
      sourceEventId: "changed-blocker",
      sourceSequence: 5,
      capturedRevision: 1,
    },
    observedRevision: 4,
    previousPosition: null,
    position: { dependentTaskId: "dependent", edgeId: "edge" },
    visited: [{ environmentId: membershipTask.environmentId, taskId: "dependent" }],
    completed: false,
  };
  const memberScan = {
    ...graphScan,
    scan: {
      kind: "group_members",
      group: membershipGroup,
      sourceEventId: "changed-group",
      sourceSequence: 6,
      capturedRevision: 4,
    },
    observedRevision: 5,
    position: { taskId: "dependent" },
    visited: [{ task: graphScan.visited[0]!, membershipGeneration: 7 }],
  };

  it("keeps accepted, uncertain and released work attached to its exact immutable task claim", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskCoordinationEvent);
    for (const event of [
      accepted,
      uncertain,
      release,
      { ...release, releaseEvidence: settledEvidence },
    ])
      expect(decode(event)).toEqual(event);
    for (const patch of [
      { previousActiveClaimId: "occupied" },
      { id: "other" },
      { commandId: "other" },
      { control: { ...accepted.control, dispatchHold: "paused" } },
      { control: { ...accepted.control, membershipRevision: 5 } },
      {
        control: {
          ...accepted.control,
          currentManager: { ...workBinding.manager, threadId: "other" },
        },
      },
      { membership: { ...membershipSnapshot, currentMembership: null } },
      {
        membership: {
          ...membershipSnapshot,
          taskRevisions: { ...membershipRevisions, lifecycleRevision: 6 },
        },
      },
      { environmentId: "foreign" },
    ])
      expect(() => decode({ ...accepted, ...patch })).toThrow();
    for (const patch of [
      { id: acceptedWork.acceptedEventId },
      { launchOrigin: initialWorkOrigin },
      { reservationId: "new" },
      { allowanceUnit: 2 },
      { reason: "\t" },
    ])
      expect(() => decode({ ...uncertain, ...patch })).toThrow();
    for (const releaseEvidence of [
      { type: "effectively_fenced", fenceId: "invented" },
      { type: "not_started", terminalRunEvent },
      { ...settledEvidence, attempt: { ...settledEvidence.attempt, providerThreadId: null } },
      { ...settledEvidence, terminalRunEvent: { ...terminalRunEvent, type: "run.created" } },
    ])
      expect(() => decode({ ...release, releaseEvidence })).toThrow();
    expect(decode({ ...uncertain, actor: "human", safe: true, approved: true })).toEqual(uncertain);
  });

  it("preserves concurrent dirty work and queue age without treating shared holds as task disqualification", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskCoordinationEvent);
    const persisted = Schema.decodeUnknownSync(task.WorkTaskPersistedCoordinationEvent);
    for (const event of [dirty, { ...dirty, membership: null }, ack])
      expect(decode(event)).toEqual(event);
    for (const patch of [
      { dirtyGeneration: 5 },
      { acknowledgedGeneration: 4 },
      {
        previousDirtyGeneration: Number.MAX_SAFE_INTEGER,
        dirtyGeneration: Number.MAX_SAFE_INTEGER + 1,
      },
    ])
      expect(() => decode({ ...dirty, ...patch })).toThrow();
    for (const patch of [
      { acknowledgedGeneration: 1 },
      { acknowledgedGeneration: 5 },
      { observedDirtyGeneration: 6 },
      { membership: { ...membership, group: { ...membershipGroup, environmentId: "foreign" } } },
    ])
      expect(() => decode({ ...ack, ...patch })).toThrow();
    expect(persisted(envelope(ready, 9))).toBeDefined();
    expect(persisted(envelope({ ...ready, previousOldestReadySequence: 9 }, 11))).toBeDefined();
    expect(() =>
      persisted(
        envelope({ ...ready, previousOldestReadySequence: 9, oldestReadySequence: 11 }, 11),
      ),
    ).toThrow();
    expect(() => persisted(envelope(ready, 10))).toThrow();
    const selected = {
      ...ready,
      kind: "accepted",
      sourceEventId: acceptedWork.acceptedEventId,
      sourceSequence: 10,
      previousOldestReadySequence: 9,
      oldestReadySequence: null,
      claim,
    };
    expect(persisted(envelope(selected, 11))).toBeDefined();
    expect(() =>
      persisted(envelope({ ...selected, previousOldestReadySequence: 15 }, 20)),
    ).toThrow();
    expect(persisted(envelope({ ...selected, previousOldestReadySequence: 10 }, 20))).toBeDefined();
    expect(() =>
      persisted(envelope({ ...selected, previousOldestReadySequence: null }, 11)),
    ).toThrow();
    const requeued = {
      ...ready,
      kind: "settled",
      sourceEventId: "release",
      sourceSequence: 40,
      previousOldestReadySequence: null,
      oldestReadySequence: 41,
      releaseEventId: "release",
      releaseSequence: 40,
      claim,
    };
    expect(persisted(envelope(requeued, 41))).toBeDefined();
    for (const releaseSequence of [9, 10, 41, 42])
      expect(() =>
        persisted(envelope({ ...requeued, sourceSequence: releaseSequence, releaseSequence }, 41)),
      ).toThrow();
    const disqualified = {
      ...ready,
      kind: "disqualified",
      reason: "task_pause",
      previousOldestReadySequence: 9,
      oldestReadySequence: null,
    };
    expect(decode(disqualified)).toBeDefined();
    for (const reason of ["group_paused", "native_hold", "unknown"])
      expect(() => decode({ ...disqualified, reason })).toThrow();
    expect(() =>
      decode({
        ...ready,
        basis: {
          ...basis,
          taskRevisions: { ...membershipRevisions, acceptedDefinitionRevision: null },
        },
      }),
    ).toThrow();
  });

  it("records bounded mutation-covered cursor positions separately from public snapshot tokens", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskCoordinationEvent);
    for (const event of [graphScan, memberScan]) {
      expect(decode(event)).toEqual(event);
      expect(
        decode({ ...event, previousPosition: event.position, visited: [], completed: true }),
      ).toBeDefined();
      for (const patch of [
        { completed: false, visited: [] },
        { observedRevision: 0 },
        { visited: [...event.visited, ...event.visited] },
        { position: null },
        { previousPosition: event.position },
        { environmentId: "foreign" },
      ])
        expect(() => decode({ ...event, ...patch })).toThrow();
    }
    for (const patch of [
      {
        visited: [membershipTask],
        position: { dependentTaskId: membershipTask.taskId, edgeId: "edge" },
      },
      { previousPosition: { dependentTaskId: membershipTask.taskId, edgeId: "edge" } },
      { position: { dependentTaskId: "other", edgeId: "edge" } },
      { position: { dependentTaskId: "dependent", edgeId: "x".repeat(2_001) } },
      {
        visited: Array.from({ length: 101 }, (_, n) => ({
          ...membershipTask,
          taskId: `task-${n}`,
        })),
      },
    ])
      expect(() => decode({ ...graphScan, ...patch })).toThrow();
    expect(() =>
      decode({ ...memberScan, visited: [{ ...memberScan.visited[0]!, membershipGeneration: 0 }] }),
    ).toThrow();
    expect(() =>
      decode({ ...graphScan, scan: { ...graphScan.scan, kind: "arbitrary_workflow" } }),
    ).toThrow();
  });

  it("rejects contradictory known journal references and exact-context substitutions", () => {
    const decode = Schema.decodeUnknownSync(task.WorkTaskPersistedCoordinationEvent);
    for (const [event, sequence] of [
      [accepted, 10],
      [uncertain, 20],
      [release, 40],
      [dirty, 11],
      [ack, 12],
      [graphScan, 30],
      [memberScan, 30],
    ] as const) {
      const value = envelope(event, sequence);
      expect(decode(value)).toEqual(value);
      for (const patch of [
        { eventId: "other" },
        { commandId: null },
        { occurredAt: "2026-11-15T00:00:00.0008Z" },
        { sequence: Number.MAX_SAFE_INTEGER + 1 },
      ])
        expect(() => decode({ ...value, context: { ...value.context, ...patch } })).toThrow();
      expect(() => decodeOldTaskEventForGraph(event)).toThrow();
    }
    expect(() => decode(envelope(accepted, 11))).toThrow();
    expect(() => decode(envelope(uncertain, 10))).toThrow();
    for (const observedSequence of [29, 40, Number.MAX_SAFE_INTEGER + 1])
      expect(() => decode(envelope({ ...release, observedSequence }, 40))).toThrow();
    const attempt = settledEvidence.attempt;
    for (const terminalAttemptEvent of [
      { ...attempt.terminalAttemptEvent, eventId: terminalRunEvent.eventId },
      { ...attempt.terminalAttemptEvent, sequence: terminalRunEvent.sequence },
      {
        ...attempt.terminalAttemptEvent,
        eventId: terminalRunEvent.eventId,
        sequence: terminalRunEvent.sequence,
      },
    ])
      expect(() =>
        decode(
          envelope(
            {
              ...release,
              releaseEvidence: {
                ...settledEvidence,
                attempt: { ...attempt, terminalAttemptEvent },
              },
            },
            40,
          ),
        ),
      ).toThrow();
    expect(() =>
      decode(
        envelope(
          { ...accepted, claim: { ...acceptedWork, sourceEventId: acceptedWork.acceptedEventId } },
          10,
        ),
      ),
    ).toThrow();
    expect(
      decode(
        envelope(
          {
            ...accepted,
            claim: {
              ...acceptedWork,
              sourceEventId: initialWorkOrigin.startEventId,
              sourceSequence: initialWorkOrigin.startSequence,
            },
          },
          10,
        ),
      ),
    ).toBeDefined();
    const witnesses = ["a", "b"].map((taskId, index) => ({
      edgeId: `edge-${index}`,
      blocker: { ...membershipTask, taskId },
      completionEventId: "same-completion",
      completionSequence: 6,
    }));
    expect(() =>
      decode(envelope({ ...accepted, claim: { ...acceptedWork, prerequisites: witnesses } }, 10)),
    ).toThrow();
  });
});
