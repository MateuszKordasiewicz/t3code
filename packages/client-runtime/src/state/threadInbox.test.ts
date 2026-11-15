import { presentThreadShell } from "./models.ts";
import { v2ThreadShell, v2Now } from "./orchestrationV2TestFixtures.ts";
import {
  EnvironmentId,
  ProviderInstanceId,
  RuntimeRequestId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  workInboxRows,
  createInboxReturnTracker,
  isThreadWorking,
  sortInboxThreadsByReturn,
  sortWorkingThreadsBySend,
} from "./threadInbox.ts";

const environmentId = EnvironmentId.make("environment-1");

function thread(id: string, working: boolean) {
  return {
    id: ThreadId.make(id),
    environmentId,
    createdAt: "2026-06-01T00:00:00.000Z",
    unsettledAt: null,
    latestRun: null,
    hasActionableProposedPlan: false,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    interactionMode: "default" as const,
    runtime: working
      ? {
          status: "running" as const,
          activeRunId: null,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerName: "Codex",
          lastError: null,
          updatedAt: "2026-06-01T00:00:00.000Z",
        }
      : null,
  };
}

describe("createInboxReturnTracker", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(10_000);
  });
  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });
  it("stamps a thread when it stops working, but never on the first observation", () => {
    const tracker = createInboxReturnTracker();
    const first = tracker.observe([thread("a", true), thread("b", false)]);
    expect(first.returnedAt(thread("a", true))).toBeUndefined();
    expect(first.returnedAt(thread("b", false))).toBeUndefined();

    const held = tracker.observe([thread("a", false), thread("b", false)]);
    vi.advanceTimersByTime(2_000);
    const second = tracker.observe([thread("a", false), thread("b", false)]);
    expect(second.returnedAt(thread("a", false))).toBeDefined();
    expect(second.returnedAt(thread("b", false))).toBeUndefined();
    expect(tracker.returnedAt(thread("a", false))).toBe(second.returnedAt(thread("a", false)));
    // An earlier observation keeps its answer, so lists built from it stay valid.
    expect(held.isWorking(thread("a", false))).toBe(true);
    expect(held.returnedAt(thread("a", false))).toBeUndefined();
  });

  it("holds only placement, then expires and notifies without another shell event", () => {
    const tracker = createInboxReturnTracker();
    const notify = vi.fn();
    const unsubscribe = tracker.subscribe(notify);
    tracker.observe([thread("a", true)]);
    tracker.observe([thread("a", false)]);
    expect(isThreadWorking(thread("a", false))).toBe(false);
    expect(tracker.isWorking(thread("a", false))).toBe(true);
    vi.advanceTimersByTime(1_999);
    tracker.observe([thread("a", false)]);
    expect(tracker.isWorking(thread("a", false))).toBe(true);
    expect(tracker.returnedAt(thread("a", false))).toBeUndefined();
    vi.advanceTimersByTime(1);
    expect(tracker.isWorking(thread("a", false))).toBe(false);
    expect(tracker.returnedAt(thread("a", false))).toBe(12_000);
    expect(notify).toHaveBeenCalledTimes(1);
    unsubscribe();
  });

  it("cancels a transient return on resume and grants a fresh grace after the next finish", () => {
    const tracker = createInboxReturnTracker();
    tracker.observe([thread("a", false)]);
    expect(tracker.isWorking(thread("a", false))).toBe(false);
    tracker.observe([thread("a", true)]);
    expect(tracker.isWorking(thread("a", true))).toBe(true);
    tracker.observe([thread("a", false)]);
    vi.advanceTimersByTime(1_500);
    tracker.observe([thread("a", true)]);
    vi.advanceTimersByTime(1_000);
    expect(tracker.returnedAt(thread("a", true))).toBeUndefined();
    tracker.observe([thread("a", false)]);
    vi.advanceTimersByTime(1_999);
    expect(tracker.isWorking(thread("a", false))).toBe(true);
    vi.advanceTimersByTime(1);
    expect(tracker.returnedAt(thread("a", false))).toBe(14_500);
  });

  it.each([
    { hasPendingApprovals: true },
    { hasPendingUserInput: true },
    { pendingSubagentRequest: "approval" as const },
    { pendingSubagentRequest: "user_input" as const },
    { pendingGuestMessageCount: 1 },
    { interactionMode: "plan" as const, hasActionableProposedPlan: true },
    { runtime: { ...thread("a", true).runtime!, status: "failed" as const, lastError: "failed" } },
    { pinnedAt: "2026-06-01T00:00:01Z" },
    { settledOverride: "settled" as const },
    { snoozedUntil: "2026-06-02T00:00:00Z" },
    { activeOrderKey: "manual-order" },
  ])("ends a grace immediately for attention or manual placement: %j", (change) => {
    const tracker = createInboxReturnTracker();
    tracker.observe([thread("a", true)]);
    tracker.observe([thread("a", false)]);
    tracker.observe([{ ...thread("a", false), ...change }]);
    expect(tracker.isWorking(thread("a", false))).toBe(false);
    expect(tracker.returnedAt(thread("a", false))).toBe(10_000);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["interrupted", "cancelled", "rolled_back"] as const)(
    "holds a %s run, because the server can still settle it",
    (status) => {
      const tracker = createInboxReturnTracker();
      const stopped = {
        ...thread("a", false),
        latestRun: {
          runId: RunId.make("stopped"),
          status,
          requestedAt: null,
          startedAt: null,
          completedAt: null,
          assistantMessageId: null,
        },
      };
      tracker.observe([thread("a", true)]);
      tracker.observe([stopped]);
      expect(tracker.isWorking(stopped)).toBe(true);
      tracker.observe([{ ...stopped, settledOverride: "settled" as const }]);
      expect(tracker.isWorking(stopped)).toBe(false);
    },
  );

  it("grants grace after a retained expired snooze", () => {
    const tracker = createInboxReturnTracker();
    const retained = {
      snoozedAt: "1970-01-01T00:00:01.000Z",
      snoozedUntil: "1970-01-01T00:00:05.000Z",
      latestRun: {
        runId: RunId.make("completed-wake"),
        status: "completed" as const,
        requestedAt: "1970-01-01T00:00:02.000Z",
        startedAt: "1970-01-01T00:00:02.000Z",
        completedAt: "1970-01-01T00:00:09.000Z",
        assistantMessageId: null,
      },
    };
    const active = { ...thread("a", true), ...retained };
    const finished = { ...thread("a", false), ...retained };
    tracker.observe([active]);
    tracker.observe([finished]);
    expect(tracker.isWorking(finished)).toBe(true);
    expect(tracker.returnedAt(finished)).toBeUndefined();
    vi.advanceTimersByTime(1_999);
    expect(tracker.isWorking(finished)).toBe(true);
    vi.advanceTimersByTime(1);
    expect(tracker.isWorking(finished)).toBe(false);
    expect(tracker.returnedAt(finished)).toBe(12_000);
  });

  it("does not grant grace to a future snooze after background completion", () => {
    const tracker = createInboxReturnTracker();
    const retained = {
      snoozedAt: "1970-01-01T00:00:01.000Z",
      snoozedUntil: "1970-01-01T00:00:30.000Z",
    };
    tracker.observe([{ ...thread("a", true), ...retained }]);
    const finished = { ...thread("a", false), ...retained };
    tracker.observe([finished]);
    expect(tracker.isWorking(finished)).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("stamps only stable returns and sorts them in their actual return order", () => {
    const tracker = createInboxReturnTracker();
    const a = { ...thread("a", false), createdAt: "1970-01-01T00:00:00Z" };
    const b = { ...thread("b", false), createdAt: "1970-01-01T00:00:00Z" };
    tracker.observe([thread("a", true), thread("b", true)]);
    tracker.observe([a, thread("b", true)]);
    vi.advanceTimersByTime(500);
    tracker.observe([a, b]);
    vi.advanceTimersByTime(1_500);
    expect(tracker.returnedAt(a)).toBe(12_000);
    expect(tracker.returnedAt(b)).toBeUndefined();
    vi.advanceTimersByTime(500);
    expect(sortInboxThreadsByReturn([a, b], tracker.returnedAt).map((entry) => entry.id)).toEqual([
      "b",
      "a",
    ]);
  });

  it("cancels pending timers for removed threads and disabled shelves", () => {
    const tracker = createInboxReturnTracker();
    const notify = vi.fn();
    tracker.subscribe(notify);
    tracker.observe([thread("a", true)]);
    tracker.observe([thread("a", false)]);
    tracker.observe([]);
    expect(vi.getTimerCount()).toBe(0);
    tracker.observe([thread("a", true)]);
    tracker.observe([thread("a", false)]);
    tracker.observe(null);
    vi.advanceTimersByTime(2_000);
    expect(notify).not.toHaveBeenCalled();
    expect(tracker.returnedAt(thread("a", false))).toBeUndefined();
  });
});

describe("sortWorkingThreadsBySend", () => {
  it("orders by the last message the user sent, not by later runs", () => {
    const sentFirst = {
      ...thread("sent-first", true),
      latestUserAuthoredMessageAt: "2026-06-01T01:00:00.000Z",
      latestRun: {
        runId: RunId.make("run:wake"),
        status: "running" as const,
        requestedAt: "2026-06-01T04:00:00.000Z",
        startedAt: "2026-06-01T04:00:00.000Z",
        completedAt: null,
        assistantMessageId: null,
      },
    };
    const sentLast = {
      ...thread("sent-last", true),
      latestUserAuthoredMessageAt: "2026-06-01T02:00:00.000Z",
    };
    const launched = { ...thread("launched", true), latestUserAuthoredMessageAt: null };
    expect(
      sortWorkingThreadsBySend([launched, sentFirst, sentLast]).map((thread) => thread.id),
    ).toEqual(["sent-last", "sent-first", "launched"]);
  });
});

const local = EnvironmentId.make("local");
const remote = EnvironmentId.make("remote");
function shell(id: string, overrides: Partial<typeof v2ThreadShell> = {}, environmentId = local) {
  return presentThreadShell(environmentId, {
    ...v2ThreadShell,
    id: ThreadId.make(id),
    lineage: { parentThreadId: null, rootThreadId: ThreadId.make(id), relationshipToParent: null },
    pendingRuntimeRequest: null,
    status: "idle",
    latestRunId: RunId.make("observed-run"),
    ...overrides,
  });
}
const request = {
  id: RuntimeRequestId.make("request"),
  kind: "user_input" as const,
  createdAt: v2Now,
};

describe("Work inbox observed threads", () => {
  it("keeps direct requests scoped, removes resolved requests and never promotes a rolled-up summary", () => {
    const parent = { ...shell("parent"), pendingSubagentRequest: "approval" as const };
    const rows = workInboxRows([
      parent,
      shell("same", { pendingRuntimeRequest: request }),
      shell("same", { pendingRuntimeRequest: request }, remote),
    ]);
    expect(rows.filter((row) => row.attention)).toHaveLength(2);
    expect(new Set(rows.map((row) => row.key)).size).toBe(3);
    expect(new Set(rows.filter((row) => row.requestKey).map((row) => row.requestKey)).size).toBe(2);
    expect(rows.find((row) => row.thread.id === parent.id)?.requestKey).toBeNull();
    expect(
      workInboxRows([
        shell("internal", {
          status: "running",
          pendingRuntimeRequest: { ...request, kind: "dynamic_tool_call" },
        }),
      ])[0],
    ).toMatchObject({ attention: false, status: "running", requestKey: null });

    expect(workInboxRows([shell("same")])[0]).toMatchObject({
      attention: false,
      requestKey: null,
      status: "idle",
    });
  });

  it("preserves actual child ownership, visible parent links, ordinary roots and orphan access", () => {
    const parent = shell("parent");
    const child = shell("child", {
      lineage: {
        parentThreadId: parent.id,
        rootThreadId: parent.id,
        relationshipToParent: "subagent",
      },
      pendingRuntimeRequest: request,
      status: "running",
    });
    const side = shell("side", {
      lineage: {
        parentThreadId: parent.id,
        rootThreadId: parent.id,
        relationshipToParent: null,
        sideChat: true,
      },
      pendingRuntimeRequest: request,
    });
    const rows = workInboxRows([parent, child, side, shell("ordinary-worker-name")]);
    expect(rows.find((row) => row.thread.id === child.id)).toMatchObject({
      attention: false,
      status: "coordinator-input",
      requestKey: null,
      parent,
    });
    expect(rows.find((row) => row.thread.id === side.id)).toMatchObject({
      attention: true,
      status: "input",
      parent,
    });
    expect(
      rows.find((row) => row.thread.id === ThreadId.make("ordinary-worker-name"))?.parent,
    ).toBeNull();
    expect(workInboxRows([child])[0]?.parent).toBeNull();
    const failedChild = shell("failed-child", {
      ...child.source,
      id: ThreadId.make("failed-child"),
      status: "failed",
      activityRunStatus: null,
    });
    expect(workInboxRows([failedChild])[0]).toMatchObject({ attention: true, status: "failed" });
    const activeChild = shell("active-child", {
      ...child.source,
      id: ThreadId.make("active-child"),
      status: "queued",
      activityRunStatus: "running",
    });
    expect(workInboxRows([activeChild])[0]).toMatchObject({
      attention: false,
      status: "coordinator-input",
    });

    expect(workInboxRows([child, shell("parent", {}, remote)])[0]?.parent).toBeNull();
    expect(workInboxRows([child, { ...parent, archivedAt: "2026-01-01" }])[0]?.parent).toBeNull();
    const approval = {
      ...child,
      source: { ...child.source, pendingRuntimeRequest: { ...request, kind: "command" as const } },
    };
    expect(workInboxRows([approval])[0]).toMatchObject({ attention: true, status: "approval" });
    const native = {
      ...approval,
      source: { ...approval.source, creationSource: "provider" as const },
    };
    expect(workInboxRows([native])[0]).toMatchObject({ attention: false, requestKey: null });
    const delegated = shell("parent", {
      delegatedFrom: { environmentId: remote, threadId: parent.id, title: "Remote parent" },
      pendingRuntimeRequest: request,
    });
    expect(workInboxRows([delegated, shell("parent", {}, remote)])[0]?.parent?.environmentId).toBe(
      remote,
    );
  });

  it("reports activity and failure without inferring Task progress or completion", () => {
    const rows = workInboxRows([
      shell("idle"),
      shell("failed", { status: "failed" }),
      shell("running", { status: "running" }),
      shell("done", { status: "completed" }),
      shell("deleted", { deletedAt: v2Now }),
    ]);
    expect(rows.map((row) => row.thread.id)).not.toContain(ThreadId.make("deleted"));
    expect(rows.find((row) => row.thread.id === ThreadId.make("failed"))).toMatchObject({
      attention: true,
      status: "failed",
    });
    expect(rows.find((row) => row.thread.id === ThreadId.make("running"))?.status).toBe("running");
    expect(rows.find((row) => row.thread.id === ThreadId.make("done"))?.status).toBe("idle");
  });
});
