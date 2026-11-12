import { describe, expect, it } from "@effect/vitest";
import { decideNativeStart } from "./WorkflowExecutionPolicy.ts";

type Input = Parameters<typeof decideNativeStart>[0];
type Owned = Extract<Input["ownership"], { kind: "owned" }>;

const owned: Owned = {
  kind: "owned",
  ownerKind: "workflow_run",
  ownerId: "owner-1",
  operationId: "operation-1",
  operationStatus: "claimed",
  operationGeneration: 1,
  ownerGeneration: 1,
  ownerStatus: "active",
  binding: { threadId: "thread-1", messageId: "message-1", nativeRunId: "run-1" },
  selected: {
    ownerKind: "workflow_run",
    ownerId: "owner-1",
    operationId: "operation-1",
    threadId: "thread-1",
    messageId: "message-1",
    nativeRunId: "run-1",
  },
  deadline: "2026-11-12T12:00:00Z",
  now: "2026-11-12T11:00:00Z",
  cancelRequested: false,
  liveGrant: "allowed",
};

describe("decideNativeStart", () => {
  it("allows only explicit ordinary or matching active owned starts", () => {
    expect(decideNativeStart({ ownership: { kind: "ordinary" } })).toEqual({
      kind: "allow",
      scope: "ordinary",
    });
    expect(decideNativeStart({ ownership: owned })).toEqual({ kind: "allow", scope: "owned" });
    expect(
      decideNativeStart({
        ownership: {
          ...owned,
          ownerKind: "planning_session",
          selected: { ...owned.selected, ownerKind: "planning_session" },
        },
      }),
    ).toEqual({ kind: "allow", scope: "owned" });
  });

  it("denies starts when ownership, identity, eligibility or authority is insufficient", () => {
    const cases: [string, Input["ownership"], string][] = [
      ["ambiguous", { kind: "ambiguous" }, "ambiguous"],
      ["unknown ownership", { kind: "unknown" } as unknown as Input["ownership"], "ambiguous"],
      ["unknown owner kind", { ...owned, ownerKind: "unknown" } as unknown as Owned, "ambiguous"],
      [
        "missing run",
        { ...owned, binding: { threadId: "thread-1", messageId: "message-1" } },
        "unbound",
      ],
      ["blank binding", { ...owned, binding: { ...owned.binding, messageId: "  " } }, "unbound"],
      [
        "blank selected owner",
        { ...owned, selected: { ...owned.selected, ownerId: "" } },
        "unbound",
      ],
      ...(
        ["ownerKind", "ownerId", "operationId", "threadId", "messageId", "nativeRunId"] as const
      ).map((key): [string, Owned, string] => [
        key,
        {
          ...owned,
          selected: {
            ...owned.selected,
            [key]: key === "ownerKind" ? "planning_session" : "other",
          },
        },
        "identity",
      ]),
      ...[NaN, Infinity, -1, 1.5, 2].map((generation): [string, Owned, string] => [
        "generation",
        { ...owned, operationGeneration: generation },
        "stale_generation",
      ]),
      ["invalid owner generation", { ...owned, ownerGeneration: NaN }, "stale_generation"],
      ...(
        ["pausing", "paused", "stopping", "stopped", "completed", "needs_attention"] as const
      ).map((status): [string, Owned, string] => [
        status,
        { ...owned, ownerStatus: status },
        "owner_inactive",
      ]),
      ...(["pending", "settled", "cancelled", "unknown"] as const).map(
        (status): [string, Owned, string] => [
          status,
          { ...owned, operationStatus: status },
          "operation_ineligible",
        ],
      ),
      ["expired", { ...owned, deadline: owned.now }, "deadline"],
      ["malformed deadline", { ...owned, deadline: "invalid" }, "deadline"],
      ["malformed now", { ...owned, now: "invalid" }, "deadline"],
      ["cancelled", { ...owned, cancelRequested: true }, "cancel_requested"],
      ...(["denied", "unavailable"] as const).map((grant): [string, Owned, string] => [
        grant,
        { ...owned, liveGrant: grant },
        "authority",
      ]),
    ];
    for (const [name, ownership, reason] of cases) {
      expect(decideNativeStart({ ownership }), name).toEqual({ kind: "deny", reason });
    }
  });
});
