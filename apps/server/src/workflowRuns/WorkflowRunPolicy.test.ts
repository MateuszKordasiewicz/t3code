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
  command: Partial<Input["command"]> = {},
  changes: { run?: Partial<Input["run"]>; facts?: Partial<Input["facts"]> } = {},
): Input => ({
  run: { ...run, ...changes.run },
  command: { ...base.command, ...command },
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
  ])("%s", (_name, value, expected) => {
    const before = structuredClone(value);
    expect(WorkflowRunPolicy.decide(value)).toMatchObject(expected);
    expect(value).toEqual(before);
  });
});
