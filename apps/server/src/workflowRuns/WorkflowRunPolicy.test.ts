import { describe, expect, it } from "@effect/vitest";
import { WorkflowOperationV1, WorkflowRunV1 } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

type Input = Parameters<typeof WorkflowRunPolicy.decide>[0];
const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const run = Schema.decodeSync(WorkflowRunV1)({
  schemaVersion: 1,
  id: "run-1",
  spec: {
    ref: { kind: "configuration", id: "spec-1", sha256: hash },
    plan: {
      planId: "plan-1",
      revisionId: "revision-1",
      revisionSchemaVersion: 1,
      revisionHash: hash,
      sourcePacketHash: hash,
    },
  },
  mode: "ordinary",
  origin: { kind: "human", id: "user-1" },
  status: "active",
  controlGeneration: 2,
  recordVersion: 5,
  allowance: {
    maxAttempts: 3,
    maxRepairRounds: 1,
    maxActiveWriters: 1,
    attemptsConsumed: 1,
    repairRoundsConsumed: 0,
    deadline: "2026-12-01T00:00:00Z",
  },
  amendmentSequence: 0,
  createdAt: "2026-11-11T00:00:00Z",
  access: { origins: [] },
});
const opId = Schema.decodeSync(WorkflowOperationV1.fields.id)("op-1");
const pending = {
  id: opId,
  fingerprint: hash,
  status: "pending",
  provablyUnstarted: true,
} as const;
const heldOperations: Input["facts"]["heldOperations"] = [];
const base: Input = {
  run,
  command: {
    kind: "pause",
    commandId: "cmd-1",
    fingerprint: hash,
    expectedVersion: 5,
    expectedGeneration: 2,
    expectedSpecHash: hash,
  },
  facts: {
    now: "2026-11-12T00:00:00Z",
    authority: true,
    custody: "none",
    activeWriterCount: 0,
    heldOperations,
    capabilitiesCurrent: true,
    compatibilityCurrent: true,
    fingerprintCurrent: true,
    originGrantCurrent: true,
  },
};
const input = (
  command: Partial<
    Extract<
      Input["command"],
      { kind: "pause" | "stop" | "deadline" | "custodySettled" | "resume" | "observeUnknown" }
    >
  > = {},
  changes: { run?: Partial<Input["run"]>; facts?: Partial<Input["facts"]> } = {},
): Input => ({
  run: { ...run, ...changes.run },
  command: { ...base.command, ...command } as Input["command"],
  facts: { ...base.facts, ...changes.facts },
});
const accepted = (status: WorkflowRunV1["status"], generation: number, actions: unknown[]) => ({
  kind: "accepted",
  run: {
    status,
    controlGeneration: generation,
    recordVersion: 6,
    allowance: run.allowance,
    spec: run.spec,
  },
  actions,
});
const rejected = (reason: string) => ({ kind: "rejected", reason });

const binding = {
  specHash: hash,
  itemId: "item-1",
  attemptId: "attempt-1",
  candidateHash: hash,
  configurationHash: hash,
  policyHash: hash,
};
const progressFacts: Partial<Input["facts"]> = {
  orderedItemIds: ["item-1", "item-2"],
  acceptedItemIds: [],
  predecessorAccepted: true,
  knownAttemptIds: [],
  knownOperationIds: [],
  currentAttemptState: "none",
  currentBinding: binding,
  acceptance: {
    binding,
    verdict: "accepted",
    evidenceAvailable: true,
    independent: true,
    finalCombined: false,
  },
  finalAccepted: false,
  publicationSettled: true,
  requiredCiPassed: true,
  publishedHead: "head",
  ciHead: "head",
};
const admit: Input["command"] = {
  ...base.command,
  kind: "admitAttempt",
  operationId: opId,
  operationFingerprint: hash,
  attemptId: "attempt-1",
  itemId: "item-1",
  repair: false,
};
const advance: Input["command"] = {
  ...base.command,
  kind: "advance",
  binding,
  finalCombined: false,
};
const complete: Input["command"] = { ...base.command, kind: "complete" };
const progress = (
  command: Input["command"],
  facts: Partial<Input["facts"]> = {},
  changes: Partial<Input["run"]> = {},
): Input => ({
  run: { ...run, ...changes },
  command,
  facts: { ...base.facts, ...progressFacts, ...facts },
});
const reserve = {
  kind: "reserve_attempt",
  operationId: opId,
  fingerprint: hash,
  attemptId: "attempt-1",
  itemId: "item-1",
  repair: false,
};
const finalBinding = { ...binding, itemId: "combined" };
const finalFacts: Partial<Input["facts"]> = {
  acceptedItemIds: ["item-1", "item-2"],
  currentBinding: finalBinding,
  acceptance: {
    binding: finalBinding,
    verdict: "accepted",
    evidenceAvailable: true,
    independent: true,
    finalCombined: true,
  },
};

describe("WorkflowRunPolicy", () => {
  it.each([
    ["version fence", input({ expectedVersion: 4 }), rejected("stale")],
    ["generation fence", input({ expectedGeneration: 1 }), rejected("stale")],
    ["spec fence", input({ expectedSpecHash: "other" }), rejected("stale")],
    [
      "receipt replay before stale",
      input(
        { expectedVersion: 4 },
        {
          facts: {
            commandReceipt: { commandId: "cmd-1", fingerprint: hash, receiptId: "receipt-1" },
          },
        },
      ),
      { kind: "replay", receiptId: "receipt-1" },
    ],
    [
      "receipt conflict before stale",
      input(
        { expectedVersion: 4 },
        {
          facts: {
            commandReceipt: { commandId: "cmd-1", fingerprint: "other", receiptId: "receipt-1" },
          },
        },
      ),
      rejected("integrity"),
    ],
    [
      "unrelated receipt",
      input(
        {},
        {
          facts: {
            commandReceipt: { commandId: "other", fingerprint: hash, receiptId: "receipt-1" },
          },
        },
      ),
      accepted("paused", 3, [{ kind: "hold_unstarted" }]),
    ],
    [
      "pause active custody",
      input({}, { facts: { custody: "active" } }),
      accepted("pausing", 3, [{ kind: "hold_unstarted" }]),
    ],
    [
      "pause unknown custody",
      input({}, { facts: { custody: "unknown" } }),
      accepted("needs_attention", 3, [{ kind: "hold_unstarted" }]),
    ],
    [
      "pause requires authority",
      input({}, { facts: { authority: false } }),
      rejected("unauthorized"),
    ],
    ["repeat pause", input({}, { run: { status: "paused" } }), rejected("invalid_state")],
    [
      "stop active custody",
      input({ kind: "stop" }, { facts: { custody: "active" } }),
      accepted("stopping", 3, [{ kind: "hold_unstarted" }, { kind: "cancel_owned" }]),
    ],
    [
      "stop unknown custody",
      input({ kind: "stop" }, { facts: { custody: "unknown" } }),
      accepted("stopping", 3, [{ kind: "hold_unstarted" }, { kind: "cancel_owned" }]),
    ],
    [
      "stop settled",
      input({ kind: "stop" }, { run: { status: "needs_attention" } }),
      accepted("stopped", 3, [{ kind: "hold_unstarted" }, { kind: "cancel_owned" }]),
    ],
    [
      "repeat stop",
      input({ kind: "stop" }, { run: { status: "stopping" } }),
      rejected("invalid_state"),
    ],
    [
      "stop requires authority",
      input({ kind: "stop" }, { facts: { authority: false } }),
      rejected("unauthorized"),
    ],
    ["early deadline", input({ kind: "deadline" }), rejected("deadline")],
    [
      "deadline at boundary",
      input(
        { kind: "deadline" },
        { facts: { now: "2026-12-01T00:00:00Z", authority: false, custody: "active" } },
      ),
      accepted("stopping", 3, [{ kind: "hold_unstarted" }, { kind: "cancel_owned" }]),
    ],
    [
      "settle pause",
      input(
        { kind: "custodySettled" },
        { run: { status: "pausing" }, facts: { authority: false } },
      ),
      accepted("paused", 2, []),
    ],
    [
      "settle stop after deadline and exhaustion",
      input(
        { kind: "custodySettled" },
        {
          run: { status: "stopping", allowance: { ...run.allowance, attemptsConsumed: 3 } },
          facts: { now: "2026-12-02T00:00:00Z" },
        },
      ),
      {
        kind: "accepted",
        run: {
          status: "stopped",
          controlGeneration: 2,
          recordVersion: 6,
          allowance: { ...run.allowance, attemptsConsumed: 3 },
        },
        actions: [],
      },
    ],
    [
      "cannot settle unknown",
      input(
        { kind: "custodySettled" },
        { run: { status: "stopping" }, facts: { custody: "unknown" } },
      ),
      rejected("custody"),
    ],
    [
      "duplicate settlement",
      input({ kind: "custodySettled" }, { run: { status: "paused" } }),
      rejected("invalid_state"),
    ],
    [
      "observe unknown",
      input({ kind: "observeUnknown" }, { facts: { custody: "unknown", authority: false } }),
      accepted("needs_attention", 2, []),
    ],
    [
      "unknown while stopping",
      input(
        { kind: "observeUnknown" },
        { run: { status: "stopping" }, facts: { custody: "unknown" } },
      ),
      accepted("stopping", 2, []),
    ],
    ["unknown requires observation", input({ kind: "observeUnknown" }), rejected("custody")],
    [
      "terminal unknown",
      input(
        { kind: "observeUnknown" },
        { run: { status: "stopped" }, facts: { custody: "unknown" } },
      ),
      rejected("invalid_state"),
    ],
    [
      "resume pending",
      input(
        { kind: "resume" },
        { run: { status: "paused" }, facts: { heldOperations: [pending] } },
      ),
      accepted("active", 3, [{ kind: "rearm", operationId: "op-1", fingerprint: hash }]),
    ],
    [
      "resume unknown",
      input(
        { kind: "resume" },
        { run: { status: "needs_attention" }, facts: { custody: "unknown" } },
      ),
      rejected("custody"),
    ],
    [
      "resume claimed",
      input(
        { kind: "resume" },
        {
          run: { status: "paused" },
          facts: { heldOperations: [pending, { ...pending, status: "claimed" }] },
        },
      ),
      rejected("custody"),
    ],
    [
      "resume cancelled",
      input(
        { kind: "resume" },
        {
          run: { status: "paused" },
          facts: { heldOperations: [{ ...pending, status: "cancelled" }] },
        },
      ),
      rejected("custody"),
    ],
    [
      "resume unknown operation",
      input(
        { kind: "resume" },
        {
          run: { status: "paused" },
          facts: { heldOperations: [{ ...pending, status: "unknown" }] },
        },
      ),
      rejected("custody"),
    ],
    [
      "resume unproven pending",
      input(
        { kind: "resume" },
        {
          run: { status: "paused" },
          facts: { heldOperations: [{ ...pending, provablyUnstarted: false }] },
        },
      ),
      rejected("custody"),
    ],
    [
      "resume active writer",
      input({ kind: "resume" }, { run: { status: "paused" }, facts: { activeWriterCount: 1 } }),
      rejected("custody"),
    ],
    [
      "resume freshness",
      input(
        { kind: "resume" },
        { run: { status: "paused" }, facts: { originGrantCurrent: false } },
      ),
      rejected("freshness"),
    ],
    [
      "resume deadline",
      input(
        { kind: "resume" },
        { run: { status: "paused" }, facts: { now: "2026-12-01T00:00:00Z" } },
      ),
      rejected("deadline"),
    ],
    [
      "resume exhausted",
      input(
        { kind: "resume" },
        { run: { status: "paused", allowance: { ...run.allowance, attemptsConsumed: 3 } } },
      ),
      rejected("exhausted"),
    ],
    [
      "resume unauthorized",
      input({ kind: "resume" }, { run: { status: "paused" }, facts: { authority: false } }),
      rejected("unauthorized"),
    ],
    [
      "admit attempt",
      progress(admit),
      {
        kind: "accepted",
        run: { recordVersion: 6, allowance: { ...run.allowance, attemptsConsumed: 2 } },
        actions: [reserve],
      },
    ],
    [
      "admit repair",
      progress(
        { ...admit, repair: true },
        { currentAttemptState: "settled_failed", currentAttemptItemId: "item-1" },
      ),
      {
        kind: "accepted",
        run: {
          recordVersion: 6,
          allowance: { ...run.allowance, attemptsConsumed: 2, repairRoundsConsumed: 1 },
        },
        actions: [{ ...reserve, repair: true }],
      },
    ],
    [
      "operation replay before stale",
      progress(
        { ...admit, expectedVersion: 4 },
        { operationReceipt: { operationId: opId, fingerprint: hash, receiptId: "receipt-op" } },
      ),
      { kind: "replay", receiptId: "receipt-op" },
    ],
    [
      "operation conflict",
      progress(admit, {
        operationReceipt: { operationId: opId, fingerprint: "other", receiptId: "receipt-op" },
      }),
      rejected("integrity"),
    ],
    ["admission paused", progress(admit, {}, { status: "paused" }), rejected("invalid_state")],
    ["admission unauthorized", progress(admit, { authority: false }), rejected("unauthorized")],
    [
      "admission at deadline",
      progress(admit, { now: "2026-12-01T00:00:00Z" }),
      rejected("deadline"),
    ],
    [
      "admission exhausted",
      progress(admit, {}, { allowance: { ...run.allowance, attemptsConsumed: 3 } }),
      rejected("exhausted"),
    ],
    [
      "repair exhausted",
      progress(
        { ...admit, repair: true },
        {},
        { allowance: { ...run.allowance, repairRoundsConsumed: 1 } },
      ),
      rejected("exhausted"),
    ],
    ["admission unknown custody", progress(admit, { custody: "unknown" }), rejected("custody")],
    ["admission writer", progress(admit, { activeWriterCount: 1 }), rejected("custody")],
    [
      "admission stale compatibility",
      progress(admit, { compatibilityCurrent: false }),
      rejected("freshness"),
    ],
    [
      "admission revoked grant",
      progress(admit, { originGrantCurrent: false }),
      rejected("freshness"),
    ],
    [
      "admission predecessor",
      progress(admit, { predecessorAccepted: false }),
      rejected("ordering"),
    ],
    ["admission order", progress({ ...admit, itemId: "item-2" }), rejected("ordering")],
    [
      "duplicate attempt",
      progress(admit, { knownAttemptIds: ["attempt-1"] }),
      rejected("integrity"),
    ],
    [
      "duplicate operation without receipt",
      progress(admit, { knownOperationIds: [opId] }),
      rejected("integrity"),
    ],
    ["repair requires settled attempt", progress({ ...admit, repair: true }), rejected("ordering")],
    [
      "accept item",
      progress(advance),
      {
        kind: "accepted",
        run: { recordVersion: 6, allowance: run.allowance },
        actions: [{ kind: "accept_item", binding }],
      },
    ],
    [
      "missing acceptance",
      {
        ...progress(advance),
        facts: { ...base.facts, orderedItemIds: ["item-1"], acceptedItemIds: [] },
      },
      rejected("evidence"),
    ],
    [
      "stale binding",
      progress(advance, { currentBinding: { ...binding, candidateHash: "stale" } }),
      rejected("evidence"),
    ],
    [
      "unavailable evidence",
      progress(advance, { acceptance: { ...progressFacts.acceptance!, evidenceAvailable: false } }),
      rejected("evidence"),
    ],
    [
      "inconclusive evidence",
      progress(advance, { acceptance: { ...progressFacts.acceptance!, verdict: "inconclusive" } }),
      rejected("evidence"),
    ],
    [
      "dependent evaluation",
      progress(advance, { acceptance: { ...progressFacts.acceptance!, independent: false } }),
      rejected("evidence"),
    ],
    ["duplicate item", progress(advance, { acceptedItemIds: ["item-1"] }), rejected("ordering")],
    ["final before all items", progress({ ...advance, finalCombined: true }), rejected("ordering")],
    [
      "accept final",
      progress({ ...advance, binding: finalBinding, finalCombined: true }, finalFacts),
      { kind: "accepted", actions: [{ kind: "accept_final", binding: finalBinding }] },
    ],
    [
      "missing final acceptance state",
      {
        ...progress({ ...advance, binding: finalBinding, finalCombined: true }, finalFacts),
        facts: { ...base.facts, ...finalFacts, orderedItemIds: ["item-1", "item-2"] },
      },
      rejected("evidence"),
    ],
    [
      "duplicate final",
      progress(
        { ...advance, binding: finalBinding, finalCombined: true },
        { ...finalFacts, finalAccepted: true },
      ),
      rejected("invalid_state"),
    ],
    [
      "complete",
      progress(complete, { finalAccepted: true }),
      {
        kind: "accepted",
        run: { status: "completed", recordVersion: 6, allowance: run.allowance },
        actions: [],
      },
    ],
    ["complete missing final", progress(complete), rejected("evidence")],
    [
      "complete unsettled publication",
      progress(complete, { finalAccepted: true, publicationSettled: false }),
      rejected("evidence"),
    ],
    [
      "complete mismatched ci",
      progress(complete, { finalAccepted: true, ciHead: "other" }),
      rejected("evidence"),
    ],
    [
      "complete active custody",
      progress(complete, { finalAccepted: true, custody: "active" }),
      rejected("custody"),
    ],
    [
      "duplicate completion",
      progress(complete, { finalAccepted: true }, { status: "completed" }),
      rejected("invalid_state"),
    ],
  ])("%s", (_name, value, expected) => {
    const before = structuredClone(value);
    expect(WorkflowRunPolicy.decide(value)).toMatchObject(expected);
    expect(value).toEqual(before);
  });
});
